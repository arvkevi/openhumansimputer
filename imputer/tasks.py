"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import os
import logging
import requests
from celery import chord, chain, group
from subprocess import Popen, PIPE
from ohapi import api
from os import environ
import pandas as pd
from django.conf import settings
from open_humans.models import OpenHumansMember
from datauploader.tasks import process_source
from openhumansimputer.settings import CHROMOSOMES
from imputer.models import ImputerMember
import bz2
import gzip
from itertools import takewhile
from openhumansimputer.celery import app


HOME = environ.get('HOME')
IMP_BIN = environ.get('IMP_BIN')
REF_PANEL = environ.get('REF_PANEL')
DATA_DIR = environ.get('DATA_DIR')
REF_FA = environ.get('REF_FA')
OUT_DIR = environ.get('OUT_DIR')

# Set up logging.
logger = logging.getLogger(__name__)

import time


@app.task(ignore_result=False)
def submit_chrom(chrom, oh_id, num_submit=0, logger=None, **kwargs):
    """
    Build and run the genipe-launcher command in Popen.
    rate_limit='1/m' sets the number of tasks per minute.
    This is important because impute2 writes a file to a shared directory that
    genipe-launcher tries to delete. If multiple tasks launch at the same time,
    celery task silently fails.
    """
    # this silly block of code runs impute2 because genipe-launcher deletes
    # two unneccesary files before they are available.
    imputer_record = ImputerMember.objects.get(oh_id=oh_id, active=True)
    imputer_record.step = 'submit_chrom'
    imputer_record.save()

    os.makedirs('{}/{}/chr{}'.format(OUT_DIR,
                                     oh_id, chrom), exist_ok=True)
    os.chdir('{}/{}/chr{}'.format(OUT_DIR, oh_id, chrom))
    run_impute_test = ['{}/impute2'.format(IMP_BIN)]
    Popen(run_impute_test, stdout=PIPE, stderr=PIPE)

    if chrom == '23':
        command = [
            'genipe-launcher',
            '--chrom', '{}'.format(chrom),
            '--bfile', '{}/{}/member.{}.plink.gt'.format(
                DATA_DIR, oh_id, oh_id),
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
            '--output-dir', '{}/{}/chr{}'.format(OUT_DIR,
                                                 oh_id, chrom),
            '--shapeit-extra', '-R {}/1000GP_Phase3_chr{}.hap.gz {}/1000GP_Phase3_chr{}.legend.gz {}/1000GP_Phase3.sample --exclude-snp {}/{}/chr{}/chr{}/chr{}.alignments.snp.strand.exclude'.format(
                REF_PANEL, chrom, REF_PANEL, chrom, REF_PANEL, OUT_DIR, oh_id, chrom, chrom, chrom)
        ]
    else:
        command = [
            'genipe-launcher',
            '--chrom', '{}'.format(chrom),
            '--bfile', '{}/{}/member.{}.plink.gt'.format(
                DATA_DIR, oh_id, oh_id),
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
            '--segment-length', '5e+06',
            '--impute2-extra', '-nind 1',
            '--report-title', '"Test"',
            '--report-number', '"Test Report"',
            '--output-dir', '{}/{}/chr{}'.format(OUT_DIR,
                                                 oh_id, chrom),
            '--shapeit-extra', '-R {}/1000GP_Phase3_chr{}.hap.gz {}/1000GP_Phase3_chr{}.legend.gz {}/1000GP_Phase3.sample --exclude-snp {}/{}/chr{}/chr{}/chr{}.alignments.snp.strand.exclude'.format(
                REF_PANEL, chrom, REF_PANEL, chrom, REF_PANEL, OUT_DIR, oh_id, chrom, chrom, chrom)
        ]

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()


@app.task(ignore_result=True)
def get_vcf(data_source_id, oh_id):
    """Download member .vcf."""
    oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
    imputer_record = ImputerMember.objects.get(oh_id=oh_id, active=True)
    imputer_record.step = 'get_vcf'
    imputer_record.save()

    user_details = api.exchange_oauth2_member(oh_member.get_access_token())
    for data_source in user_details['data']:
        if str(data_source['id']) == str(data_source_id):
            data_file_url = data_source['download_url']
    datafile = requests.get(data_file_url)
    os.makedirs('{}/{}'.format(DATA_DIR, oh_id), exist_ok=True)
    with open('{}/{}/member.{}.vcf'.format(DATA_DIR, oh_id, oh_id), 'wb') as handle:
        if '.bz2' in data_file_url:
            textobj = bz2.decompress(datafile.content)
            handle.write(textobj)
        elif '.gz' in data_file_url:
            textobj = gzip.decompress(datafile.content)
            handle.write(textobj)
        else:
            for block in datafile.iter_content(1024):
                handle.write(block)


@app.task(ignore_result=False)
def prepare_data(oh_id):
    """Process the member's .vcf."""
    imputer_record = ImputerMember.objects.get(oh_id=oh_id, active=True)
    imputer_record.step = 'prepare_data'
    imputer_record.save()

    command = [
        'imputer/prepare_genotypes.sh', '{}'.format(oh_id)
    ]
    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()


def _rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


@app.task(ignore_result=False)
def combine_chrom(oh_id, num_submit=0, logger=None, **kwargs):
    """
    1. read .impute2 files (w/ genotype probabilities)
    2. read .impute2_info files (with "info" field for filtering)
    3. filter the genotypes in .impute2_info
    4. merge on right (.impute2_info), acts like a filter for the left.
    """
    print('{} Imputation has completed, now combining results.'.format(oh_id))
    impute_cols = ['chr', 'name', 'position',
                   'a0', 'a1', 'a0a0_p', 'a0a1_p', 'a1a1_p']

    df = pd.DataFrame()
    df_gp = pd.DataFrame()  # hold genotype probabilities
    for chrom in CHROMOSOMES:
        df_impute = pd.read_csv('{}/{}/chr{}/chr{}/final_impute2/'
                                'chr{}.imputed.impute2'
                                .format(OUT_DIR, oh_id, chrom, chrom, chrom),
                                sep=' ',
                                header=None,
                                names=impute_cols)

        df_info = pd.read_csv(
            '{}/{}/chr{}/chr{}/final_impute2/chr{}.imputed.impute2_info'.format(
                OUT_DIR, oh_id, chrom, chrom, chrom),
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

    df_gp.to_csv('{}/{}/member.imputed.impute2.GP'.format(OUT_DIR, oh_id),
                 header=True,
                 index=False,
                 sep=' ')

    # dump all chromosomes as an .impute2
    df.to_csv('{}/{}/member.imputed.impute2'.format(OUT_DIR, oh_id),
              header=False,
              index=False,
              sep=' ')
    # don't need this huge dataframe anymore
    del df
    print('{} finished combining results, now converting to .vcf'.format(oh_id))

    # convert to vcf
    os.chdir(settings.BASE_DIR)
    output_vcf_cmd = [
        'imputer/output_vcf.sh', '{}'.format(oh_id)
    ]
    process = Popen(output_vcf_cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    # annotate genotype probabilities and info metric
    vcf_file = '{}/{}/member.imputed.vcf'.format(OUT_DIR, oh_id)
    cols = ['CHROM', 'POS', 'ID', 'REF', 'ALT',
            'QUAL', 'FILTER', 'INFO', 'FORMAT', 'MEMBER']
    with open(vcf_file, 'r') as vcf:
        headiter = takewhile(lambda s: s.startswith('#'), vcf)
        header = list(headiter)
        dfvcf = pd.read_csv(vcf_file, sep='\t', header=None,
                            comment='#', names=cols)

    df_gp.rename(columns={'name': 'ID'}, inplace=True)
    df_gp.set_index(['ID'], inplace=True)
    dfvcf.set_index(['ID'], inplace=True)
    dfvcf = dfvcf.merge(
        df_gp[['a0a0_p', 'a0a1_p', 'a1a1_p', 'info']], left_index=True, right_index=True)
    del df_gp
    dfvcf['MEMBER'] = dfvcf['MEMBER'] + ':' + dfvcf['a0a0_p'].round(3).astype(
        str) + ',' + dfvcf['a0a1_p'].round(3).astype(str) + ',' + dfvcf['a1a1_p'].round(3).astype(str)
    dfvcf['FORMAT'] = dfvcf['FORMAT'].astype(str) + ':GP'
    dfvcf['INFO'] = dfvcf['INFO'].astype(
        str) + ';INFO=' + dfvcf['info'].round(3).astype(str)
    dfvcf.reset_index(inplace=True)
    new_header = ['##FORMAT=<ID=GP,Number=3,Type=Float,Description="Estimated Posterior Probabilities (rounded to 3 digits) for Genotypes 0/0, 0/1 and 1/1">\n',
                  '##INFO=<ID=INFO,Number=1,Type=Float,Description="Impute2 info metric">\n'
                  ]
    header.insert(-2, new_header[0])
    header.insert(-4, new_header[1])
    with open(vcf_file, 'w') as vcf:
        for line in header:
            vcf.write(line)
    dfvcf[cols].to_csv(vcf_file, sep='\t', header=None, index=False, mode='a')

    print('{} finished converting to .vcf, now uploading to OpenHumans'.format(oh_id))
    # upload file to OpenHumans
    process_source(oh_id)

    # Message Member
    oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
    project_page = environ.get('OH_ACTIVITY_PAGE')
    api.message('Open Humans Imputation Complete',
                'Check {} to see your imputed genotype results from Open Humans.'.format(
                    project_page),
                oh_member.access_token,
                project_member_ids=[oh_id])
    print('{} emailed member'.format(oh_id))

    # clean users files
    os.chdir(settings.BASE_DIR)
    clean_command = [
        'imputer/clean_files.sh', '{}'.format(oh_id)
    ]
    process = Popen(clean_command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    print('{} finished removing files'.format(oh_id))

    imputer_record = ImputerMember.objects.get(oh_id=oh_id, active=True)
    imputer_record.step = 'complete'
    imputer_record.active = False
    imputer_record.save()

@app.task
def pipeline(vcf_id, oh_id):
    task1 = get_vcf.si(vcf_id, oh_id)
    task2 = prepare_data.si(oh_id)
    async_chroms = group(submit_chrom.si(chrom, oh_id) for chrom in CHROMOSOMES)
    task3 = chord(async_chroms, combine_chrom.si(oh_id))

    pipeline = chain(task1, task2, task3)()
