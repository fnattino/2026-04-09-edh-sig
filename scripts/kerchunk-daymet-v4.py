# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "dcachefs",
#     "fsspec",
#     "h5py",
#     "kerchunk",
# ]
# ///
import json
import pathlib

import dcachefs  # noqa: F401
import fsspec

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr


# NOTE: accessing dCache requires authentication, see
DCACHE_DAYMET_ROOT = "/pnfs/grid.sara.nl/data/remotesensing/disk/daymet-daily-v4/"
DAYMET_REGION = "na"  # North America
START_YEAR = 1980
END_YEAR = 2019
OUTPUT_DIR = pathlib.Path("data/daymet-daily-v4/region-na/")


def get_hdf_paths() -> list[str]:
    """Collect the paths of all NetCDF/HDF5 files on dCache."""
    fs = fsspec.filesystem("dcache")
    return [path
        for year in range(START_YEAR, END_YEAR+1)
        for path in fs.glob(f"{DCACHE_DAYMET_ROOT}/region-{DAYMET_REGION}/{DAYMET_REGION}-{year}/*.nc")
    ]


def write_reference_file(hdf_path: str) -> str:
    """Extract the chunk references from the NetCDF/HDF5, write them as JSON."""
    json_file = pathlib.Path(hdf_path).with_suffix(".json").name
    json_path = OUTPUT_DIR / json_file
    if not json_path.exists():
        print(f"Extracting references from file: {hdf_path}")
        chunks = SingleHdf5ToZarr(f"dcache://{hdf_path}", inline_threshold=300)
        with json_path.open("wb") as f:
            f.write(json.dumps(chunks.translate()).encode())
    return json_path.as_posix()


def combine_reference_files(json_paths: list[str]) -> None:
    """Combine all chunk references into a single JSON file."""
    combined_json_path = OUTPUT_DIR / f"region-{DAYMET_REGION}.json"
    if not combined_json_path.exists():
        print("Combining references ...")
        chunks = MultiZarrToZarr(
            json_paths,
            concat_dims=["time"],
            identical_dims=["x", "y", "nv"],
        )
        refs = chunks.translate()
        with combined_json_path.open(mode="wb") as f:
            f.write(json.dumps(refs).encode())


def main():
    # collect the paths of all the NetCDF/HDF5 files on dCache
    hdf_paths = get_hdf_paths()

    # extract the chunk references for allNetCDF/HDF5 files, save them as JSON
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    reference_files = [
        write_reference_file(hdf_path)
        for hdf_path in hdf_paths
    ]

    # combine all the chunk references in a single JSON file
    combine_reference_files(reference_files)


if __name__ == "__main__":
    main()
