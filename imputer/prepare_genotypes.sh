#!/bin/bash

# Variant Length
VARLEN="$2"

# Check if chr23 or chrX is included.
INPUT_CHROMS=$(grep -v '^#' "$DATA_DIR"/"$1"/member."$1".vcf | awk '{ print $1 }' | sort | uniq)
MISSING_X=true
if [[ $INPUT_CHROMS == *"23"* || $INPUT_CHROMS == *"X"* || $INPUT_CHROMS == *"chrX"* ]];
then MISSING_X=false
fi

# Check for 11 columns in the vcf header row
NCOL=$(grep '^#CHROM' "$DATA_DIR"/"$1"/member."$1".vcf | awk '{print NF}' - | sort -nu | tail -n 1)
if [ "$NCOL" -eq "11" ]; then
 TENCOLHEADER=$(grep '^#CHROM' "$DATA_DIR"/"$1"/member."$1".vcf | awk OFS="\t" '{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10$11}')
 sed -i "/^#CHROM/c$TENCOLHEADER" "$DATA_DIR"/"$1"/member."$1".vcf
fi
# grab the header
grep '^#' "$DATA_DIR"/"$1"/member."$1".vcf > "$DATA_DIR"/"$1"/member."$1".header
# drop duplicate records
sort -u -k1,1 -k2,2 "$DATA_DIR"/"$1"/member."$1".vcf | grep -v '^#' >  "$DATA_DIR"/"$1"/member."$1".uniq.noheader.vcf
#add the header back to the uniqued file
cat "$DATA_DIR"/"$1"/member."$1".header "$DATA_DIR"/"$1"/member."$1".uniq.noheader.vcf > "$DATA_DIR"/"$1"/member."$1".uniq.unsorted.vcf
#sort the uniq vcf
grep "^#" "$DATA_DIR"/"$1"/member."$1".uniq.unsorted.vcf > "$DATA_DIR"/"$1"/member."$1".uniq.vcf && grep -v "^#" "$DATA_DIR"/"$1"/member."$1".uniq.unsorted.vcf | \
  sort -V -k1,1 -k2,2n >> "$DATA_DIR"/"$1"/member."$1".uniq.vcf
# filter for only PASS and '.' FILTER
gawk -F '\t' '{if($0 ~ /\#/) print; else if($7 == "PASS" || $7 == ".") print}' "$DATA_DIR"/"$1"/member."$1".uniq.vcf > tmp && mv tmp "$DATA_DIR"/"$1"/member."$1".uniq.vcf
# convert to plink format
"$IMP_BIN"/plink2 \
--vcf "$DATA_DIR"/"$1"/member."$1".uniq.vcf \
--geno \
--make-bed \
--const-fid member_"$1" \
--vcf-half-call 'r' \
--set-all-var-ids @:#[b37]\$r,\$a \
--new-id-max-allele-len "$VARLEN" 'truncate' \
--rm-dup 'force-first' \
--max-alleles 2 \
--fa "$REF_FA"/hg19.fasta \
--ref-from-fa 'force' \
--normalize \
--out "$DATA_DIR"/"$1"/member."$1".plink

sed -i 's/X\t/23\t/g' "$DATA_DIR"/"$1"/member."$1".plink.bim
sed -i 's/X:/23:/g' "$DATA_DIR"/"$1"/member."$1".plink.bim

if ! $MISSING_X ; then
    # convert to plink 1
    "$IMP_BIN"/plink \
    --bfile "$DATA_DIR"/"$1"/member."$1".plink \
    --geno \
    --impute-sex ycount \
    --allow-extra-chr \
    --make-bed \
    --out "$DATA_DIR"/"$1"/member."$1".plink.sorted \

    # set missing var ids
    "$IMP_BIN"/plink \
    --bfile "$DATA_DIR"/"$1"/member."$1".plink.sorted \
    --geno \
    --impute-sex ycount \
    --allow-extra-chr \
    --make-bed \
    --set-missing-var-ids @:\#[b37]\$1,\$2 \
    --out "$DATA_DIR"/"$1"/member."$1".plink.gt
else
    # No Sex Imputation
    # convert to plink 1
    "$IMP_BIN"/plink \
    --bfile "$DATA_DIR"/"$1"/member."$1".plink \
    --geno \
    --allow-extra-chr \
    --make-bed \
    --out "$DATA_DIR"/"$1"/member."$1".plink.sorted \

    # set missing var ids
    "$IMP_BIN"/plink \
    --bfile "$DATA_DIR"/"$1"/member."$1".plink.sorted \
    --geno \
    --allow-extra-chr \
    --make-bed \
    --set-missing-var-ids @:\#[b37]\$1,\$2 \
    --out "$DATA_DIR"/"$1"/member."$1".plink.gt
fi