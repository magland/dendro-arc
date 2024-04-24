import dendro_arc as arc


def test_import_from_local():
    project_id = "392126db"  # arc1
    local_file_name = '/home/magland/Downloads/sub-BH494_ses-20230817T132836_ecephys.nwb'
    project_file_name = 'imported/000/sub-BH494/sub-BH494_ses-20230817T132836_ecephys.nwb.lindi.json'

    arc.import_local_nwb_file(
        project_id=project_id,
        file_name=local_file_name,
        project_file_name=project_file_name
    )


if __name__ == "__main__":
    test_import_from_local()
