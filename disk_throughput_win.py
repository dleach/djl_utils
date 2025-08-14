# disk_throughput_win.py
# Measure unbuffered (sector-level) read/write throughput on Windows, including very small volumes.
# Now supports multiple passes and/or minimum-duration runs.

import argparse, os, time, ctypes, statistics, textwrap
from ctypes import wintypes

# --- Win32 constants ---
GENERIC_READ   = 0x80000000
GENERIC_WRITE  = 0x40000000
FILE_SHARE_READ  = 0x00000001
FILE_SHARE_WRITE = 0x00000002
CREATE_ALWAYS  = 2
OPEN_EXISTING  = 3
FILE_ATTRIBUTE_NORMAL   = 0x00000080
FILE_FLAG_NO_BUFFERING  = 0x20000000
FILE_FLAG_WRITE_THROUGH = 0x80000000

INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

CreateFileW = kernel32.CreateFileW
CreateFileW.argtypes = [
    wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
    wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE
]
CreateFileW.restype = wintypes.HANDLE

CloseHandle = kernel32.CloseHandle
CloseHandle.argtypes = [wintypes.HANDLE]
CloseHandle.restype  = wintypes.BOOL

ReadFile = kernel32.ReadFile
ReadFile.argtypes = [
    wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD,
    ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID
]
ReadFile.restype = wintypes.BOOL

WriteFile = kernel32.WriteFile
WriteFile.argtypes = [
    wintypes.HANDLE, wintypes.LPCVOID, wintypes.DWORD,
    ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID
]
WriteFile.restype = wintypes.BOOL

SetFilePointerEx = kernel32.SetFilePointerEx
SetFilePointerEx.argtypes = [wintypes.HANDLE, ctypes.c_longlong, ctypes.POINTER(ctypes.c_longlong), wintypes.DWORD]
SetFilePointerEx.restype  = wintypes.BOOL

SetEndOfFile = kernel32.SetEndOfFile
SetEndOfFile.argtypes = [wintypes.HANDLE]
SetEndOfFile.restype  = wintypes.BOOL

GetDiskFreeSpaceW = kernel32.GetDiskFreeSpaceW
GetDiskFreeSpaceW.argtypes = [
    wintypes.LPCWSTR,
    ctypes.POINTER(wintypes.DWORD),
    ctypes.POINTER(wintypes.DWORD),
    ctypes.POINTER(wintypes.DWORD),
    ctypes.POINTER(wintypes.DWORD),
]
GetDiskFreeSpaceW.restype = wintypes.BOOL

GetDiskFreeSpaceExW = kernel32.GetDiskFreeSpaceExW
GetDiskFreeSpaceExW.argtypes = [
    wintypes.LPCWSTR,
    ctypes.POINTER(ctypes.c_ulonglong),  # lpFreeBytesAvailable
    ctypes.POINTER(ctypes.c_ulonglong),  # lpTotalNumberOfBytes
    ctypes.POINTER(ctypes.c_ulonglong),  # lpTotalNumberOfFreeBytes
]
GetDiskFreeSpaceExW.restype = wintypes.BOOL

VirtualAlloc = kernel32.VirtualAlloc
VirtualAlloc.argtypes = [wintypes.LPVOID, ctypes.c_size_t, wintypes.DWORD, wintypes.DWORD]
VirtualAlloc.restype  = wintypes.LPVOID
MEM_RESERVE = 0x2000
MEM_COMMIT  = 0x1000
PAGE_READWRITE = 0x04

def win_err():
    err = ctypes.get_last_error()
    return f"WinError {err}: {ctypes.WinError(err)}"

def get_sector_size(dir_path: str) -> int:
    sectors_per_cluster = wintypes.DWORD()
    bytes_per_sector    = wintypes.DWORD()
    num_free_clusters   = wintypes.DWORD()
    total_num_clusters  = wintypes.DWORD()
    ok = GetDiskFreeSpaceW(
        dir_path,
        ctypes.byref(sectors_per_cluster),
        ctypes.byref(bytes_per_sector),
        ctypes.byref(num_free_clusters),
        ctypes.byref(total_num_clusters),
    )
    if not ok:
        raise OSError(f"GetDiskFreeSpaceW failed for {dir_path} - {win_err()}")
    return bytes_per_sector.value

def get_free_bytes(dir_path: str) -> int:
    free_avail = ctypes.c_ulonglong()
    total      = ctypes.c_ulonglong()
    free_total = ctypes.c_ulonglong()
    ok = GetDiskFreeSpaceExW(dir_path, ctypes.byref(free_avail), ctypes.byref(total), ctypes.byref(free_total))
    if not ok:
        raise OSError(f"GetDiskFreeSpaceExW failed for {dir_path} - {win_err()}")
    return free_avail.value

def open_unbuffered(path: str, access: int, create_disposition: int, write_through: bool = False):
    flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING
    if write_through:
        flags |= FILE_FLAG_WRITE_THROUGH
    handle = CreateFileW(
        path, access, FILE_SHARE_READ | FILE_SHARE_WRITE, None,
        create_disposition, flags, None
    )
    if handle == INVALID_HANDLE_VALUE:
        raise OSError(f"CreateFileW failed for '{path}' - {win_err()}")
    return handle

def set_file_size(handle, size_bytes: int):
    new_pos = ctypes.c_longlong()
    if not SetFilePointerEx(handle, size_bytes, ctypes.byref(new_pos), 0):
        raise OSError(f"SetFilePointerEx (size) failed - {win_err()}")
    if not SetEndOfFile(handle):
        raise OSError(f"SetEndOfFile failed - {win_err()}")
    if not SetFilePointerEx(handle, 0, ctypes.byref(new_pos), 0):
        raise OSError(f"SetFilePointerEx (rewind) failed - {win_err()}")

def set_pointer(handle, offset: int = 0):
    new_pos = ctypes.c_longlong()
    if not SetFilePointerEx(handle, offset, ctypes.byref(new_pos), 0):
        raise OSError(f"SetFilePointerEx failed - {win_err()}")

def alloc_aligned(size_bytes: int):
    ptr = VirtualAlloc(None, size_bytes, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)
    if not ptr:
        raise MemoryError("VirtualAlloc failed")
    return ptr

def throughput_mb_s(total_bytes: int, seconds: float) -> float:
    return (total_bytes / (1024 * 1024)) / seconds if seconds > 0 else 0.0

def run_write_tests(path: str, file_size: int, block_size: int, passes: int, min_seconds: float):
    # Open once, size once; loop writes with rewind between passes.
    h = None
    try:
        h = open_unbuffered(path, GENERIC_READ | GENERIC_WRITE, CREATE_ALWAYS, write_through=True)
        set_file_size(h, file_size)
        buf = alloc_aligned(block_size)
        ctypes.memset(buf, 0xA5, block_size)

        per_pass = []
        total_elapsed = 0.0
        done = 0
        while (done < passes) or (total_elapsed < min_seconds):
            set_pointer(h, 0)
            to_write = file_size
            dw_written = wintypes.DWORD()
            start = time.perf_counter()
            while to_write > 0:
                this_io = block_size if to_write >= block_size else to_write
                ok = WriteFile(h, buf, this_io, ctypes.byref(dw_written), None)
                if not ok or dw_written.value != this_io:
                    raise OSError(f"WriteFile failed/wrote {dw_written.value} - {win_err()}")
                to_write -= this_io
            elapsed = time.perf_counter() - start
            total_elapsed += elapsed
            per_pass.append(throughput_mb_s(file_size, elapsed))
            done += 1
        return per_pass
    finally:
        if h:
            CloseHandle(h)

def run_read_tests(path: str, file_size: int, block_size: int, passes: int, min_seconds: float):
    h = None
    try:
        h = open_unbuffered(path, GENERIC_READ, OPEN_EXISTING, write_through=False)
        buf = alloc_aligned(block_size)

        per_pass = []
        total_elapsed = 0.0
        done = 0
        while (done < passes) or (total_elapsed < min_seconds):
            set_pointer(h, 0)
            to_read = file_size
            dw_read = wintypes.DWORD()
            start = time.perf_counter()
            while to_read > 0:
                this_io = block_size if to_read >= block_size else to_read
                ok = ReadFile(h, buf, this_io, ctypes.byref(dw_read), None)
                if not ok or dw_read.value != this_io:
                    raise OSError(f"ReadFile failed/read {dw_read.value} - {win_err()}")
                to_read -= this_io
            elapsed = time.perf_counter() - start
            total_elapsed += elapsed
            per_pass.append(throughput_mb_s(file_size, elapsed))
            done += 1
        return per_pass
    finally:
        if h:
            CloseHandle(h)

def parse_size(args) -> int:
    if args.size_bytes is not None:
        return int(args.size_bytes)
    if args.size_kb is not None:
        return int(args.size_kb) * 1024
    return int(args.size_mb) * 1024 * 1024

def align_down(x, multiple):
    return (x // multiple) * multiple

def align_up(x, multiple):
    return ((x + multiple - 1) // multiple) * multiple

def main():
    p = argparse.ArgumentParser(
        description="Measure sector-level disk throughput on Windows using unbuffered I/O.",
        epilog=textwrap.dedent("""\
        Examples:
          disk_throughput_win.py --target X:\\ --size-kb 48 --passes 53
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--target", default="C:\\", help="Directory/drive to test (e.g., X:\\). Test file is created here.")
    p.add_argument("--size-mb", type=int, default=1024, help="Test file size in MiB (default: 1024).")
    p.add_argument("--size-kb", type=int, help="Test file size in KiB (overrides --size-mb).")
    p.add_argument("--size-bytes", type=int, help="Test file size in bytes (overrides --size-kb/--size-mb).")
    p.add_argument("--block-kb", type=int, default=1024, help="I/O block size in KiB (auto-reduced if larger than file).")
    p.add_argument("--keep", action="store_true", help="Keep the test file after running.")
    p.add_argument("--no-autofit", action="store_true", help="Disable auto-fit to free space (may fail if too large).")
    p.add_argument("--passes", type=int, default=1, help="Number of passes for each test (write/read).")
    p.add_argument("--min-seconds", type=float, default=0.0, help="Run each test until total time ≥ this many seconds (in addition to --passes).")
    args = p.parse_args()

    target_dir = os.path.abspath(args.target)
    if not os.path.isdir(target_dir):
        raise SystemExit(f"Target '{target_dir}' is not a directory. For a drive, use e.g. X:\\")

    sector_size = get_sector_size(target_dir)
    free_bytes = get_free_bytes(target_dir)

    requested = parse_size(args)
    print(f"Requested: {requested}")
    max_bytes = align_down(int(free_bytes * (0 if args.no_autofit else 0.8)), sector_size) if not args.no_autofit else requested
    print(f"max_bytes: {max_bytes}")

    if requested > free_bytes and args.no_autofit:
        raise SystemExit(f"Requested size ({requested} B) exceeds free space ({free_bytes} B). Remove --no-autofit or choose a smaller size.")

    file_size = requested
    if not args.no_autofit and (requested == 0 or requested > max_bytes):
        file_size = max_bytes

    if file_size < sector_size:
        file_size = sector_size

    block_size = args.block_kb * 1024
    if block_size % sector_size != 0:
        block_size = align_up(block_size, sector_size)
    if block_size > file_size:
        block_size = file_size
    file_size = align_down(file_size, block_size)
    if file_size == 0:
        file_size = block_size

    test_path = os.path.join(target_dir, "diskperf_test.bin")

    print(f"Sector size: {sector_size} bytes")
    print(f"Free space:  {free_bytes} bytes")
    print(f"Testing on:  {test_path}")
    print(f"File size:   {file_size} bytes ({file_size/1024:.1f} KiB)")
    print(f"Block size:  {block_size} bytes ({block_size/1024:.1f} KiB)")
    print(f"Passes:      {args.passes}  |  Min seconds: {args.min_seconds:.2f}")

    try:
        w_passes = run_write_tests(test_path, file_size, block_size, args.passes, args.min_seconds)
        r_passes = run_read_tests(test_path,  file_size, block_size, args.passes, args.min_seconds)

        def summarize(title, values):
            vmin = min(values)
            vmax = max(values)
            vavg = statistics.fmean(values)
            print(f"\n{title}:")
            for i, v in enumerate(values, 1):
                print(f"  Pass {i}: {v:.2f} MB/s")
            print(f"  -> min/avg/max: {vmin:.2f} / {vavg:.2f} / {vmax:.2f} MB/s")

        summarize("Write throughput (unbuffered, write-through)", w_passes)
        summarize("Read throughput  (unbuffered)", r_passes)

    finally:
        if not args.keep:
            try:
                os.remove(test_path)
            except Exception:
                pass

if __name__ == "__main__":
    main()
