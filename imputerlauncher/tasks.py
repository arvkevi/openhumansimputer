"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import logging
import os
import json
import shutil
import tempfile
import textwrap
import requests
from celery import shared_task
from django.conf import settings
from open_humans.models import OpenHumansMember
from datetime import datetime
from demotemplate.settings import rr
from subprocess import Popen, PIPE
from ohapi import api
from os import environ

HOME = environ.get('HOME')
IMP_BIN = environ.get('IMP_BIN')
REF_PANEL = environ.get('REF_PANEL')
DATA_DIR = environ.get('DATA_DIR')
REF_FA = environ.get('REF_FA')
OUT_DIR = environ.get('OUT_DIR')

# Set up logging.
logger = logging.getLogger(__name__)


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

@shared_task
def submit_chrom(chrom, num_submit=0, logger=None, **kwargs):
    """Build and run the genipe-launcher command in Popen."""

    command = [
        'genipe-launcher',
        '--chrom', '{}'.format(chrom),
        '--bfile', '{}/{}'.format(DATA_DIR, 'member.plink.gt'),
        '--shapeit-bin', '{}/shapeit'.format(IMP_BIN),
        '--impute2-bin', '{}/impute2'.format(IMP_BIN),
        '--plink-bin', '{}/plink'.format(IMP_BIN),
        '--reference', '{}/hg19.fasta'.format(REF_FA),
        '--hap-template', '{}/1000GP_Phase3_chr{}.hap.gz'.format(REF_PANEL, chrom),
        '--legend-template', '{}/1000GP_Phase3_chr{}.legend.gz'.format(REF_PANEL, chrom),
        '--map-template', '{}/genetic_map_chr{}_combined_b37.txt'.format(REF_PANEL, chrom),
        '--sample-file', '{}/1000GP_Phase3.sample'.format(REF_PANEL),
        '--filtering-rules', 'ALL<0.01', 'ALL>0.99',
        '--report-title', '"Test"',
        '--report-number', '"Test Report"',
        '--output-dir', '{}'.format(OUT_DIR),
        '--shapeit-extra', '-R {}/1000GP_Phase3_chr{}.hap.gz {}/1000GP_Phase3_chr{}.legend.gz {}/1000GP_Phase3.sample --exclude-snp {}/chr{}/chr{}.alignments.snp.strand.exclude'.format(REF_PANEL, chrom, REF_PANEL, chrom, REF_PANEL, OUT_DIR, chrom, chrom)
        ]

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    #try:
        #process = Popen(command, stdout=PIPE, stderr=PIPE)
    #    stdout, stderr = process.communicate()

    #except Exception as e:
    #    print(e)
    #    print('oops something went wrong in submit_chrom')


def get_vcf(oh_member):
    """Download member .vcf."""
    user_details = api.exchange_oauth2_member(oh_member.get_access_token())
    for data_source in user_details['data']:
        if 'vcf' in data_source['metadata']['tags'] and '23andMe' in data_source['metadata']['tags']:
            data_file_url = data_source['download_url']

    file_23andme = requests.get(data_file_url)
    with open('{}/member.vcf.gz'.format(DATA_DIR), 'wb') as handle:
        for block in file_23andme.iter_content(1024):
            handle.write(block)


def prepare_data():
    """
    Process the .vcf.
    Maybe just call prepare_vcf.sh from Popen

    uniq 23andMe-genotyping.vcf > 23andMe-genotyping.uniq.vcf
    plink --vcf 23andMe-genotyping.uniq.vcf --out ka.gt
    plink --bfile ka.gt --maf 0.01 --make-bed --set-missing-var-ids @:#[b37]\$1,\$2 --out ka.gt.maf
    1. gunzip
    2. plink
    3. plink 2
    4. name these member.__
    """
    print(os.listdir())
    command = [
    'imputerlauncher/prepare_genotypes.sh'
    ]
    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()


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


def make_datafile(user_data, metadata, tempdir):
    """
    Make a user data file in the tempdir.
    """
    filename = 'user_data_' + datetime.today().strftime('%Y%m%d')
    filepath = os.path.join(tempdir, filename)

    with open(filepath, 'w') as f:
        f.write(user_data)

    return filepath, metadata


def make_example_datafile(tempdir):
    """
    Make a lorem-ipsum file in the tempdir, for demonstration purposes.
    """
    filepath = os.path.join(tempdir, 'example_data2.txt')
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
        settings.OH_DELETE_FILES,
        params={'access_token': oh_member.get_access_token()},
        data={'project_member_id': oh_member.oh_id,
              'file_basename': filename})
    req.raise_for_status()


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
        settings.OH_DIRECT_UPLOAD, oh_member.get_access_token())
    req1 = requests.post(
        upload_url,
        data={'project_member_id': oh_member.oh_id,
              'filename': os.path.basename(filepath),
              'metadata': json.dumps(metadata)})
    req1.raise_for_status()

    # Upload to S3 target.
    with open(filepath, 'rb') as fh:
        req2 = requests.put(url=req1.json()['url'], data=fh)
    req2.raise_for_status()

    # Report completed upload to Open Humans.
    complete_url = ('{}?access_token={}'.format(
        settings.OH_DIRECT_UPLOAD_COMPLETE, oh_member.get_access_token()))
    req3 = requests.post(
        complete_url,
        data={'project_member_id': oh_member.oh_id,
              'file_id': req1.json()['id']})
    req3.raise_for_status()

    logger.debug('Upload done: "{}" for member {}.'.format(
            os.path.basename(filepath), oh_member.oh_id))


@shared_task
def make_request_respectful_get(url, realms, **kwargs):
    r = rr.get(url=url, realms=realms, **kwargs)
    logger.debug('Request completed. Response: {}'.format(r.text))
