"""
A template for an asynchronous task that updates data in Open Humans.

This example task:
  1. deletes any current files in OH if they match the planned upload filename
  2. adds a data file
"""
from __future__ import absolute_import, print_function

import json
import os
import shutil
import tempfile
import textwrap
from urllib2 import HTTPError

from celery import shared_task
from django.utils import lorem_ipsum
import requests

from .models import OpenHumansMember

OH_API_BASE = 'https://www.openhumans.org/api/direct-sharing'
OH_EXCHANGE_TOKEN = OH_API_BASE + '/project/exchange-member/'
OH_DELETE_FILES = OH_API_BASE + '/project/files/delete/'
OH_DIRECT_UPLOAD = OH_API_BASE + '/project/files/upload/direct/'
OH_DIRECT_UPLOAD_COMPLETE = OH_API_BASE + '/project/files/upload/complete/'


@shared_task
def xfer_to_open_humans(oh_id, num_submit=0, logger=None, **kwargs):
    """
    Transfer data to Open Humans.

    num_submit is an optional parameter in case you want to resubmit failed
    tasks (see comments in code).
    """
    print('Trying to copy data for {} to Open Humans'.format(oh_id))
    oh_member = OpenHumansMember.objects.get(oh_id=oh_id)

    # Make a tempdir for all temporary files.
    # Delete this even if an exception occurs.
    tempdir = tempfile.mkdtemp()
    try:
        add_data_to_open_humans(oh_member, tempdir)
    finally:
        shutil.rmtree(tempdir)

    # Note: Want to re-run tasks in case of a failure?
    # You can resubmit a task by calling it again. (Be careful with recursion!)
    # e.g. to give up, resubmit, & try again after 10s if less than 5 attempts:
    # if num_submit < 5:
    #     num_submit += 1
    #     xfer_to_open_humans.apply_async(
    #         args=[oh_id, num_submit], kwargs=kwargs, countdown=10)
    #     return


def add_data_to_open_humans(oh_member, tempdir):
    """
    Add demonstration file to Open Humans.

    This might be a good place to start editing, to add your own project data.

    This template is written to provide the function with a tempdir that
    will be cleaned up later. You can use the tempdir to stage the creation of
    files you plan to upload to Open Humans.
    """
    # Create example file.
    data_filepath, data_metadata = make_example_datafile(tempdir)

    # Remove any files with this name previously added to Open Humans.
    delete_oh_file_by_name(oh_member, filename=os.path.basename(data_filepath))

    # Upload this file to Open Humans.
    upload_file_to_oh(oh_member, data_filepath, data_metadata)


def make_example_datafile(tempdir):
    """
    Make a lorem-ipsum file in the tempdir, for demonstration purposes.
    """
    filepath = os.path.join(tempdir, 'example_data.txt')
    paras = lorem_ipsum.paragraphs(3, common=True)
    output_text = '\n'.join(['\n'.join(textwrap.wrap(p)) for p in paras])
    with open(filepath, 'w') as f:
        f.write(output_text)
    metadata = {
        'tags': ['example', 'text', 'demo'],
        'description': 'File with lorem ipsum text for demonstration purposes',
    }
    return filepath, metadata


def delete_oh_file_by_name(oh_member, filename):
    """
    Delete all project files matching the filename for this Open Humans member.

    This deletes files this project previously added to the Open Humans
    member account, if they match this filename. Read more about file deletion
    API options here:
    https://www.openhumans.org/direct-sharing/oauth2-data-upload/#deleting-files
    """
    req = requests.post(
        OH_DELETE_FILES,
        params={'access_token': oh_member.get_access_token()},
        data={'project_member_id': oh_member.oh_id,
              'file_basename': filename})


def upload_file_to_oh(oh_member, filepath, metadata):
    """
    This demonstrates using the Open Humans "large file" upload process.

    The small file upload process is simpler, but it can time out. This
    alternate approach is required for large files, and still appropriate
    for small files.

    This process is "direct to S3" using three steps: 1. get S3 target URL from
    Open Humans, 2. Perform the upload, 3. Notify Open Humans when complete.
    """
    # Get the S3 target from Open Humans.
    upload_url = '{}?access_token={}'.format(
        OH_DIRECT_UPLOAD, oh_member.get_access_token())
    req1 = requests.post(
        upload_url,
        data={'project_member_id': oh_member.oh_id,
              'filename': os.path.basename(filepath),
              'metadata': json.dumps(metadata)})
    if req1.status_code != 201:
        raise HTTPError(code=req1.status_code,
                        text='Bad response when starting file upload.')

    # Upload to S3 target.
    with open(filepath, 'rb') as fh:
        req2 = requests.put(url=req1.json()['url'], data=fh)
    if req2.status_code != 200:
        raise HTTPError(code=req2.status_code,
                        text='Bad response when uploading to target.')

    # Report completed upload to Open Humans.
    complete_url = ('{}?access_token={}'.format(
        OH_DIRECT_UPLOAD_COMPLETE, oh_member.get_access_token()))
    req3 = requests.post(
        complete_url,
        data={'project_member_id': oh_member.oh_id,
              'file_id': req1.json()['id']})
    if req3.status_code != 200:
        raise HTTPError(code=req2.status_code,
                        text='Bad response when uploading to target.')

    print('Upload done: "{}" for member {}.'.format(
        os.path.basename(filepath), oh_member.oh_id))
