
from dandi import dandiarchive as da
import dendro_arc as arc


def test_import_from_dandi():
    project_id = "392126db"  # arc1
    dandiset_id = "000629"
    dandiset_version = "draft"
    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return
        for asset in assets:
            print(asset)
            asset_id = asset.identifier
            arc.import_dandi_nwb_file(
                project_id=project_id,
                dandiset_id=dandiset_id,
                dandiset_version=dandiset_version,
                asset_path=asset.path,
                asset_id=asset_id,
            )
            break


if __name__ == "__main__":
    test_import_from_dandi()
