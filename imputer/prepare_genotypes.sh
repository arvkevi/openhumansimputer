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
# filter for only PASS and '.' FILTER
gawk -F '\t' '{if($0 ~ /\#/) print; else if($7 == "PASS" || $7 == ".") print}' "$DATA_DIR"/"$1"/member."$1".uniq.vcf > tmp && mv tmp "$DATA_DIR"/"$1"/member."$1".uniq.vcf
# convert to plink format
"$IMP_BIN"/plink2 \
--vcf "$DATA_DIR"/"$1"/member."$1".uniq.vcf \
--geno \
--make-bed \
--vcf-half-call 'r' \
--set-all-var-ids @:#[b37]\$r,\$a \
--new-id-max-allele-len 1000 'truncate' \
--rm-dup 'force-first' \
--max-alleles 2 \
--fa "$REF_FA"/hg19.fasta \
--ref-from-fa 'force' \
--normalize \
--out "$DATA_DIR"/"$1"/member."$1".plink 

sed -i 's/X\t/23\t/g' "$DATA_DIR"/"$1"/member."$1".plink.bim
sed -i 's/X:/23:/g' "$DATA_DIR"/"$1"/member."$1".plink.bim

# convert to plink 1 
"$IMP_BIN"/plink \
--bfile "$DATA_DIR"/"$1"/member."$1".plink \
--geno \
--impute-sex ycount \
--allow-extra-chr \
--make-bed \
--set-missing-var-ids @:\#[b37]\$1,\$2 \
--out "$DATA_DIR"/"$1"/member."$1".plink.gt \
