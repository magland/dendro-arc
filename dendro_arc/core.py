import tempfile
import lindi
import dendro.client as dc


def import_dandi_nwb_file(
    *,
    project_id: str,
    dandiset_id: str,
    dandiset_version: str,
    asset_path: str,
    asset_id: str,
):
    with tempfile.TemporaryDirectory() as tmpdir:
        project = dc.load_project(project_id)
        url = f"https://api.dandiarchive.org/api/assets/{asset_id}/download/"
        x = lindi.LindiH5pyFile.from_hdf5_file(url, local_cache=lindi.LocalCache())
        tmp_fname = f"{tmpdir}/test.lindi.json"
        x.write_lindi_file(tmp_fname)
        blob_url = dc.upload_file_blob(project=project, file_name=tmp_fname)
        if not blob_url:
            print(f"Error uploading {tmp_fname}")
            return
        dc.set_file(
            project=project,
            file_name=f"imported/{dandiset_id}/{asset_path}.lindi.json",
            url=blob_url,
            metadata={"dandisetId": dandiset_id, "dandisetVersion": dandiset_version},
        )


def import_local_nwb_file(
    *,
    project_id: str,
    file_name: str,
    project_file_name: str
):
    if file_name.endswith('.nwb.lindi.json'):
        project = dc.load_project(project_id)
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_area = lindi.StagingArea.create(tmpdir)
            x = lindi.LindiH5pyFile.from_lindi_file(file_name, staging_area=staging_area)
            assert x.staging_store is not None
            print('Copying chunks to temporary staging area')
            x.staging_store.copy_chunks_to_staging_area(download_remote=False)

            print('Consolidating chunks in staging area')
            x.staging_store.consolidate_chunks()

            def on_upload_blob(fname: str):
                return dc.upload_file_blob(project=project, file_name=fname)

            def on_upload_main(fname: str):
                return dc.upload_file_blob(project=project, file_name=fname)

            print('Uploading')
            url = x.staging_store.upload(
                on_upload_blob=on_upload_blob,
                on_upload_main=on_upload_main,
                consolidate_chunks=True
            )
            print('Setting project file')
            dc.set_file(
                project=project,
                file_name=project_file_name,
                url=url,
                metadata={}
            )
    elif file_name.endswith('.nwb'):
        zarr_store_opts = lindi.LindiH5ZarrStoreOpts(
            num_dataset_chunks_threshold=None  # be sure to include all the chunks
        )
        x = lindi.LindiH5pyFile.from_hdf5_file(
            file_name,
            zarr_store_opts=zarr_store_opts
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_fname = f"{tmpdir}/tmp.nwb.lindi.json"
            print('Creating lindi.json file')
            x.write_lindi_file(tmp_fname)
            return import_local_nwb_file(project_id=project_id, file_name=tmp_fname, project_file_name=project_file_name)
    else:
        raise Exception(f"Unsupported file type for {file_name}")
