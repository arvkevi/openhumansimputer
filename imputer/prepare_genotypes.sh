#!/bin/bash

echo "$DATA_DIR"
# grab the header
grep '^#' "$DATA_DIR"/"$1"/member."$1".vcf > "$DATA_DIR"/"$1"/member."$1".header
# drop duplicate records
sort -u -k1,1 -k2,2 "$DATA_DIR"/"$1"/member."$1".vcf | grep -v '^#' >  "$DATA_DIR"/"$1"/member."$1".uniq.noheader.vcf
#add the header back to the uniqued file
cat "$DATA_DIR"/"$1"/member."$1".header "$DATA_DIR"/"$1"/member."$1".uniq.noheader.vcf > "$DATA_DIR"/"$1"/member."$1".uniq.unsorted.vcf
#sort the uniq vcf
grep "^#" "$DATA_DIR"/"$1"/member."$1".uniq.unsorted.vcf > "$DATA_DIR"/"$1"/member."$1".uniq.vcf && grep -v "^#" "$DATA_DIR"/"$1"/member."$1".uniq.unsorted.vcf | \
  sort -V -k1,1 -k2,2n >> "$DATA_DIR"/"$1"/member."$1".uniq.vcf
# only keep PASS FILTER variants or not evaluated '.'
gawk -i -F '\t' '{if($0 ~ /\#/) print; else if($7 == "PASS" || $7 == ".") print}' "$DATA_DIR"/"$1"/member."$1".uniq.vcf

# convert to plink format
"$IMP_BIN"/plink \
--vcf "$DATA_DIR"/"$1"/member."$1".uniq.vcf \
--impute-sex ycount \
--geno \
--make-bed \
--out "$DATA_DIR"/"$1"/member."$1".plink \
# remove missing ids
"$IMP_BIN"/plink \
--bfile "$DATA_DIR"/"$1"/member."$1".plink \
--geno \
--make-bed \
--set-missing-var-ids @:\#[b37]\$1,\$2 \
--out "$DATA_DIR"/"$1"/member."$1".plink.gt \
