#!/bin/bash

gunzip -c "$HOME"/data/member."$1".vcf.gz > "$HOME"/data/member."$1".vcf
# drop duplicate records
uniq "$HOME"/data/member."$1".vcf > "$HOME"/data/member."$1".uniq.vcf
# convert to plink format
"$HOME"/impbin/plink \
--vcf "$HOME"/data/member"$1".uniq.vcf \
--impute-sex ycount \
--make-bed \
--out "$HOME"/data/member."$1".plink
# remove missing ids
"$HOME"/impbin/plink \
--bfile "$HOME"/data/member."$1".plink \
--maf 0.01 \
--make-bed \
--set-missing-var-ids @:\#[b37]\$1,\$2 \
--out "$HOME"/data/member."$1".plink.gt
