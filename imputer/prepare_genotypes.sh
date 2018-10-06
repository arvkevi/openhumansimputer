#!/bin/bash

# drop duplicate records
uniq "$DATA_DIR"/"$1"/member."$1".vcf > "$DATA_DIR"/"$1"/member."$1".uniq.vcf
# convert to plink format
"$IMP_BIN"/plink \
--vcf "$DATA_DIR"/"$1"/member."$1".uniq.vcf \
--impute-sex ycount \
--geno \
--make-bed \
--out "$DATA_DIR"/"$1"/member."$1".plink
# remove missing ids
"$IMP_BIN"/plink \
--bfile "$DATA_DIR"/"$1"/member."$1".plink \
--maf 0.01 \
--geno \
--make-bed \
--set-missing-var-ids @:\#[b37]\$1,\$2 \
--out "$DATA_DIR"/"$1"/member."$1".plink.gt
