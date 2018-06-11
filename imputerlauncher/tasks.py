"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import logging
import requests
from celery import shared_task
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
def submit_chrom(chrom, num_submit=0, logger=None, **kwargs):
    """Build and run the genipe-launcher command in Popen."""
    if chrom == 'X':
        pass
    else:
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
    """Process the member's .vcf."""
    command = [
        'imputerlauncher/prepare_genotypes.sh'
    ]
    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()


@shared_task
def combine_chrom(num_submit=0, logger=None, **kwargs):
    # why does this print to log immediately?
    print('Everything has completed')
    command = [
        'cominbe chr vcfs'
    ]
