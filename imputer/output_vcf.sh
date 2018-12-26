#!/bin/bash

# copy and fix .sample file

sed -i'.bak' '3s/0/-9/g' "$OUT_DIR"/"$1"/chr"$2"/chr"$2"/final_impute2/chr"$2".imputed.sample

# convert to vcf and
"$IMP_BIN"/plink2 \
--gen "$OUT_DIR"/"$1"/chr"$2"/chr"$2"/final_impute2/chr"$2".imputed.impute2 \
--sample "$OUT_DIR"/"$1"/chr"$2"/chr"$2"/final_impute2/chr"$2".imputed.sample \
--hard-call-threshold 0.4 \
--export vcf \
--ref-from-fa 'force' \
--fa "$REF_FA"/hg19.fasta \
--missing-code -9 \
--out "$OUT_DIR"/"$1"/chr"$2"/chr"$2"/final_impute2/chr"$2".member.imputed
