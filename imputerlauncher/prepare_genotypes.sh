#!/bin/bash

gunzip -c "$HOME"/data/member.vcf.gz > "$HOME"/data/member.vcf
# drop duplicate records
uniq "$HOME"/data/member.vcf > "$HOME"/data/member.uniq.vcf
# convert to plink format
"$HOME"/impbin/plink \
--vcf "$HOME"/data/member.uniq.vcf \
--out "$HOME"/data/member.plink
# remove missing ids
"$HOME"/impbin/plink \
--bfile "$HOME"/data/member.plink \
--maf 0.01 \
--make-bed \
--set-missing-var-ids @:\#[b37]\$1,\$2 \
--out "$HOME"/data/member.plink.gt
