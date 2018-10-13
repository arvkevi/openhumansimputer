#!/bin/bash

if [[ $TEST_CHROMS = true ]]
then
cat "$OUT_DIR"/"$1"/chr5/chr5/final_impute2/chr5.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr6/chr6/final_impute2/chr6.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr7/chr7/final_imputer2/chr7.member.imputed.vcf.bz2 \
>"$OUT_DIR"/"$1"/member.imputed.vcf.bz2
fi

if [[ $TEST_CHROMS = false ]]
then
cat "$OUT_DIR"/"$1"/chr1/chr1/final_impute2/chr1.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr2/chr2/final_impute2/chr2.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr3/chr3/final_impute2/chr3.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr4/chr4/final_impute2/chr4.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr5/chr5/final_impute2/chr5.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr6/chr6/final_impute2/chr6.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr7/chr7/final_impute2/chr7.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr8/chr8/final_impute2/chr8.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr9/chr9/final_impute2/chr9.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr10/chr10/final_impute2/chr10.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr11/chr11/final_impute2/chr11.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr12/chr12/final_impute2/chr12.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr13/chr13/final_impute2/chr13.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr14/chr14/final_impute2/chr14.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr15/chr15/final_impute2/chr15.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr16/chr16/final_impute2/chr16.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr17/chr17/final_impute2/chr17.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr18/chr18/final_impute2/chr18.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr19/chr19/final_impute2/chr19.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr20/chr20/final_impute2/chr20.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr21/chr21/final_impute2/chr21.member.imputed.vcf.bz2 \
"$OUT_DIR"/"$1"/chr22/chr22/final_impute2/chr22.member.imputed.vcf.bz2 \
>"$OUT_DIR"/"$1"/member.imputed.vcf.bz2
fi
