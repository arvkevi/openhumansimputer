#!/bin/bash

# drop duplicate records
uniq "$HOME"/data/"$1"/member."$1".vcf > "$HOME"/data/"$1"/member."$1".uniq.vcf
# convert to plink format
"$HOME"/impbin/plink \
--vcf "$HOME"/data/"$1"/member."$1".uniq.vcf \
--impute-sex ycount \
--make-bed \
--out "$HOME"/data/"$1"/member."$1".plink
# remove missing ids
"$HOME"/impbin/plink \
--bfile "$HOME"/data/"$1"/member."$1".plink \
--maf 0.01 \
--make-bed \
--set-missing-var-ids @:\#[b37]\$1,\$2 \
--out "$HOME"/data/"$1"/member."$1".plink.gt
