#!/bin/bash

# copy and fix .sample file

cp "$OUT_DIR"/chr22/chr22/final_impute2/chr22.imputed.sample "$OUT_DIR"/member.sample
sed -i'.bak' '3s/0/-9/g' "$OUT_DIR"/member.sample

# convert to vcf and
"$IMP_BIN"/plink \
--gen "$OUT_DIR"/member.imputed.impute2 \
--sample "$OUT_DIR"/member.sample \
--export vcf \
--ref-from-fa "$REF_FA"/hg19.fasta \
--missing-code -9 \
--out "$OUT_DIR"/member.imputed
