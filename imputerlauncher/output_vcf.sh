#!/bin/bash

# copy and fix .sample file

cp "$OUT_DIR"/"$1"/chr22/chr22/final_impute2/chr22.imputed.sample "$OUT_DIR"/"$1"/member.sample
sed -i'.bak' '3s/0/-9/g' "$OUT_DIR"/"$1"/member.sample

# convert to vcf and
"$IMP_BIN"/plink2 \
--gen "$OUT_DIR"/"$1"/member.imputed.impute2 \
--sample "$OUT_DIR"/"$1"/member.sample \
--export vcf \
--ref-from-fa "$REF_FA"/hg19.fasta \
--missing-code -9 \
--out "$OUT_DIR"/"$1"/member.imputed

# remove user files
rm "DATA_DIR"/member."$1"*
#rm -rf "$OUT_DIR"/"$1"
