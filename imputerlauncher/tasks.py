"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import os
import logging
import requests
from celery import shared_task
from subprocess import Popen, PIPE
from ohapi import api
from os import environ
import pandas as pd
from demotemplate.settings import CHROMOSOMES


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
    """
    Build and run the genipe-launcher command in Popen.
    rate_limit='1/m' sets the number of tasks per minute.
    This is important because impute2 writes a file to a shared directory that
    genipe-launcher tries to delete. If multiple tasks launch at the same time,
    celery task silently fails.
    """
    # this silly block of code runs impute2 because genipe-launcher deletes
    # two unneccesary files before they are available.
    os.makedirs('{}/chr{}'.format(OUT_DIR, chrom), exist_ok=True)
    os.chdir('{}/chr{}'.format(OUT_DIR, chrom))
    run_impute_test = ['{}/impute2'.format(IMP_BIN)]
    Popen(run_impute_test, stdout=PIPE, stderr=PIPE)

    if chrom == '23':
        command = [
            'genipe-launcher',
            '--chrom', '{}'.format(chrom),
            '--bfile', '{}/{}'.format(DATA_DIR, 'member.plink.gt'),
            '--shapeit-bin', '{}/shapeit'.format(IMP_BIN),
            '--impute2-bin', '{}/impute2'.format(IMP_BIN),
            '--plink-bin', '{}/plink'.format(IMP_BIN),
            '--reference', '{}/hg19.fasta'.format(REF_FA),
            '--hap-template', '{}/1000GP_Phase3_chr{}.hap.gz'.format(
                REF_PANEL, chrom),
            '--legend-template', '{}/1000GP_Phase3_chr{}.legend.gz'.format(
                REF_PANEL, chrom),
            '--map-template', '{}/genetic_map_chr{}_combined_b37.txt'.format(
                REF_PANEL, chrom),
            '--sample-file', '{}/1000GP_Phase3.sample'.format(REF_PANEL),
            '--filtering-rules', 'ALL<0.01', 'ALL>0.99',
            '--report-title', '"Test"',
            '--report-number', '"Test Report"',
            '--output-dir', '{}/chr{}'.format(OUT_DIR, chrom),
            '--shapeit-extra', '-R {}/1000GP_Phase3_chr{}.hap.gz {}/1000GP_Phase3_chr{}.legend.gz {}/1000GP_Phase3.sample --exclude-snp {}/chr{}/chr{}/chr{}.alignments.snp.strand.exclude'.format(
                REF_PANEL, chrom, REF_PANEL, chrom, REF_PANEL, OUT_DIR, chrom, chrom, chrom)
        ]
    else:
        command = [
            'genipe-launcher',
            '--chrom', '{}'.format(chrom),
            '--bfile', '{}/{}'.format(DATA_DIR, 'member.plink.gt'),
            '--shapeit-bin', '{}/shapeit'.format(IMP_BIN),
            '--impute2-bin', '{}/impute2'.format(IMP_BIN),
            '--plink-bin', '{}/plink'.format(IMP_BIN),
            '--reference', '{}/hg19.fasta'.format(REF_FA),
            '--hap-template', '{}/1000GP_Phase3_chr{}.hap.gz'.format(
                REF_PANEL, chrom),
            '--legend-template', '{}/1000GP_Phase3_chr{}.legend.gz'.format(
                REF_PANEL, chrom),
            '--map-template', '{}/genetic_map_chr{}_combined_b37.txt'.format(
                REF_PANEL, chrom),
            '--sample-file', '{}/1000GP_Phase3.sample'.format(REF_PANEL),
            '--filtering-rules', 'ALL<0.01', 'ALL>0.99',
            '--impute2-extra', '-nind 1',
            '--report-title', '"Test"',
            '--report-number', '"Test Report"',
            '--output-dir', '{}/chr{}'.format(OUT_DIR, chrom),
            '--shapeit-extra', '-R {}/1000GP_Phase3_chr{}.hap.gz {}/1000GP_Phase3_chr{}.legend.gz {}/1000GP_Phase3.sample --exclude-snp {}/chr{}/chr{}/chr{}.alignments.snp.strand.exclude'.format(
                REF_PANEL, chrom, REF_PANEL, chrom, REF_PANEL, OUT_DIR, chrom, chrom, chrom)
        ]

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()


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


def _rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


@shared_task
def combine_chrom(num_submit=0, logger=None, **kwargs):
    """
    1. read .impute2 files (w/ genotype probabilities)
    2. read .impute2_info files (with "info" field for filtering)
    3. filter the genotypes in .impute2_info
    4. merge on right (.impute2_info), acts like a filter for the left.
    """
    print('Imputation has completed, now combining results...')

    impute_cols = ['chr', 'name', 'position',
                   'a0', 'a1', 'a0a0_p', 'a0a1_p', 'a1a1_p']

    df = pd.DataFrame()
    df_gp = pd.DataFrame()  # hold genotype probabilities
    for chrom in CHROMOSOMES:
        df_impute = pd.read_csv('{}/chr{}/chr{}/final_impute2/'
                                'chr{}.imputed.impute2'
                                .format(OUT_DIR, chrom, chrom, chrom),
                                sep=' ',
                                header=None,
                                names=impute_cols)

        df_info = pd.read_csv(
            '{}/chr{}/chr{}/final_impute2/chr{}.imputed.impute2_info'.format(
                OUT_DIR, chrom, chrom, chrom),
            sep='\t')

        # combine impute2 and impute2_info to induce filter
        df_merged = df_impute.merge(
            df_info, on=['chr', 'position', 'a0', 'a1'], how='right')
        df_merged.rename(columns={'name_x': 'name'}, inplace=True)
        df_gp = pd.concat([df_gp, df_merged])
        df_merged = df_merged[impute_cols]
        df = pd.concat([df, df_merged])

    df_gp['name'] = df_gp['name'].apply(_rreplace, args=(':', '_', 2))
    df['name'] = df['name'].apply(_rreplace, args=(':', '_', 2))

    df_gp.to_csv('{}/member.imputed.impute2.GP'.format(OUT_DIR),
                 header=True,
                 index=False,
                 sep=' ')

    # dump all chromosomes as an .impute2
    df.to_csv('{}/member.imputed.impute2'.format(OUT_DIR),
              header=False,
              index=False,
              sep=' ')
    print('finished combining results, now converting to .vcf')

    # convert to vcf
    gen_to_vcf = [
        '{}/plink'.format(IMP_BIN),
        '--gen', '{}/member.imputed.impute2'.format(OUT_DIR),
        '--sample', '{}/chr{}/chr{}/final_impute2/chr{}.imputed.sample'.format(
            OUT_DIR, chrom, chrom, chrom),
        '--recode', 'vcf',
        '--out', '{}/member.imputed'.format(OUT_DIR)
    ]
    Popen(gen_to_vcf, stdout=PIPE, stderr=PIPE)
    print('finished converting to .vcf')
