import gc
import os

import pandas as pd
from tqdm import tqdm


# If this script runs out of memory, decrease this number.
CHUNK_SIZE = 100_000


def _generate_intermediate_csv():
    """
    Generate intermediate CSV files. This is separated into a function to make clearing memory easier.
    """

    # Load taxonomy
    taxa = pd.read_csv("data/inaturalist-taxonomy.dwca/taxa.csv")
    taxa = taxa[taxa["taxonRank"] == "species"]

    # Load common names
    english_vernacular = pd.read_csv(
        "data/inaturalist-taxonomy.dwca/VernacularNames-english.csv"
    )

    # Match scientific names to common names, dropping rows without common names
    english_taxa = taxa.merge(
        english_vernacular[["id", "vernacularName"]], how="left", on="id"
    )
    english_taxa = english_taxa[english_taxa["vernacularName"].notnull()][
        ["scientificName", "vernacularName"]
    ]

    # Each scientific name can have multiple common names. Keep first
    english_taxa = english_taxa.groupby("scientificName").first().reset_index()

    # Process observations and keep id and scientific name. Merge common names and drop
    # rows without English common names. Save file to disk.

    def process_chunk_observations(chunk_df: pd.DataFrame) -> pd.DataFrame:
        chunk_df = chunk_df[chunk_df["datasetName"] == "iNaturalist research-grade observations"]
        chunk_df = chunk_df[["id", "scientificName"]].merge(
            english_taxa, how="left", on="scientificName"
        )
        return chunk_df[chunk_df["vernacularName"].notnull()]

    p_bar = tqdm(
        total=os.path.getsize("data/gbif-observations-dwca/observations.csv"),
        desc=f"Creating out/common_name_observations.csv",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,  # make use of standard units e.g. KB, MB, etc.
        miniters=1,
    )
    last_tell = 0

    with open("out/common_name_observations.csv", "w") as fp_out, open(
            "data/gbif-observations-dwca/observations.csv", "rb"
    ) as fp_in:

        chunk_gen = pd.read_csv(fp_in, chunksize=CHUNK_SIZE)
        process_chunk_observations(next(chunk_gen)).to_csv(fp_out, index=False)

        current_tell = fp_in.tell()
        p_bar.update(current_tell - last_tell)
        last_tell = current_tell

        for chunk in chunk_gen:
            process_chunk_observations(chunk).to_csv(fp_out, index=False, header=False)

            current_tell = fp_in.tell()
            p_bar.update(current_tell - last_tell)
            last_tell = current_tell

    p_bar.close()

    # Process media and keep id, catalogNumber, and format where media is CC0-licensed and an image. Save
    # file to disk.

    def process_chunk_cc0_photos(chunk_df: pd.DataFrame) -> pd.DataFrame:
        return chunk_df[
            (chunk_df["license"] == "http://creativecommons.org/publicdomain/zero/1.0/")
            & (chunk_df["type"] == "StillImage")
            ][["id", "catalogNumber", "format"]]

    p_bar = tqdm(
        total=os.path.getsize("data/gbif-observations-dwca/media.csv"),
        desc=f"Creating out/cc0_photos.csv",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        miniters=1,
    )
    last_tell = 0

    with open("out/cc0_photos.csv", "w") as fp_out, open(
            "data/gbif-observations-dwca/media.csv", "rb"
    ) as fp_in:

        chunk_gen = pd.read_csv(fp_in, chunksize=CHUNK_SIZE)
        process_chunk_cc0_photos(next(chunk_gen)).to_csv(fp_out, index=False)

        current_tell = fp_in.tell()
        p_bar.update(current_tell - last_tell)
        last_tell = current_tell

        for chunk in chunk_gen:
            process_chunk_cc0_photos(chunk).to_csv(fp_out, index=False, header=False)

            current_tell = fp_in.tell()
            p_bar.update(current_tell - last_tell)
            last_tell = current_tell


if __name__ == "__main__":
    _generate_intermediate_csv()
    gc.collect()

    # Merge common names and image identifier.

    common_name_observations = pd.read_csv("out/common_name_observations.csv")
    cc0_photos = pd.read_csv("out/cc0_photos.csv")

    cc0_common_name = cc0_photos.merge(common_name_observations, how="left", on="id")

    cc0_common_name[
        cc0_common_name["scientificName"].notnull()
        & cc0_common_name["vernacularName"].notnull()
    ].to_csv("out/cc0_photos_with_common_name.csv", index=False)
