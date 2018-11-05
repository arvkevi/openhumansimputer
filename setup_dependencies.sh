#!/usr/bin/bash
# this file gets sourced at the start of the app
# after the heroku config vars.

source .env

IMP_BIN="$BASE_DATA_DIR"/impbin
REF_PANEL="$BASE_DATA_DIR"/1000GP_Phase3
DATA_DIR="$BASE_DATA_DIR"/data
REF_FA="$BASE_DATA_DIR"/hg19
OUT_DIR="$BASE_DATA_DIR"/genipe_output


if [ ! -d "$IMP_BIN" ]; then
  mkdir -p $IMP_BIN

  echo DOWNLOADING IMPUTE2...
  curl https://mathgen.stats.ox.ac.uk/impute/impute_v2.3.2_x86_64_static.tgz | \
  tar -xz -C $IMP_BIN --no-anchored --strip-components=1 impute_v2.3.2_x86_64_static/impute2

  echo DOWNLOADING PLINK...
  # plink 1.x
  curl https://www.cog-genomics.org/static/bin/plink181012/plink_linux_x86_64.zip -o $TMP_DIR/plink_linux_x86_64.zip
  unzip $TMP_DIR/plink_linux_x86_64.zip plink -d $IMP_BIN
  rm -f $TMP_DIR/plink_linux_x86_64.zip

  # plink 2.x for recoding .gen to .vcf
  curl http://s3.amazonaws.com/plink2-assets/alpha1/plink2_linux_x86_64.zip -o $TMP_DIR/plink2_linux_x86_64.zip
  unzip $TMP_DIR/plink2_linux_x86_64.zip plink2 -d $IMP_BIN
  rm -f $TMP_DIR/plink2_linux_x86_64.zip

  echo DOWNLOADING SHAPEIT...
  curl https://mathgen.stats.ox.ac.uk/genetics_software/shapeit/shapeit.v2.r837.GLIBCv2.12.Linux.static.tgz | \
  tar -xz -C $IMP_BIN --no-anchored --strip-components=1 bin/shapeit
fi

if [ ! -d "$REF_PANEL" ]; then
  echo DOWNLOADING 1kG HAPLOTYPES...
  mkdir -p $REF_PANEL

  curl https://mathgen.stats.ox.ac.uk/impute/1000GP_Phase3.tgz -o $TMP_DIR/1000GP_Phase3.tgz
  tar -xvzf $TMP_DIR/1000GP_Phase3.tgz -C $REF_PANEL
  rm -f $TMP_DIR/1000GP_Phase3.tgz

  # uncomment below when implementing X chromosome imputation
  #echo DOWNLOADING 1kG chrX HAPLOTYPES...
  #mkdir "$REF_PANEL"_chrX
  #cd "$REF_PANEL"_chrX
  #wget https://mathgen.stats.ox.ac.uk/impute/1000GP_Phase3_chrX.tgz
  #tar -xvzf 1000GP_Phase3_chrX.tgz
  #cd
fi

if [ ! -d "$REF_FA" ]; then
  echo DOWNLOADING HG19...
  mkdir -p $REF_FA
  wget -P $REF_FA http://statgen.org/wp-content/uploads/Softwares/genipe/supp_files/hg19.tar.bz2
  bzip2 -d $REF_FA/hg19.tar.bz2
  tar xvf $REF_FA/hg19.tar
fi

# this is where the OH user data will exist
mkdir $DATA_DIR
# this is where the output from the imputation pipeline will exist
mkdir $OUT_DIR
