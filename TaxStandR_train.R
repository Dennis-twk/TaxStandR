rm(list = ls()); gc()

library(TaxStandR)
library(stringr)
library(dplyr)
library(openxlsx)
library(writexl)


# NCBI数据库载入密钥可以加快检索速度，注意不能有空格，Sys.getenv("ENTREZ_KEY")替换为: "你的密钥"
## NCBI密钥获取链接： https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/
# 例如：options(ENTREZ_KEY = "651b9638111ab101bc031ca572fbd440a008") # 36个字符
## 没有 API 密钥的情况下每秒 3 个请求，有API就每秒10个请求
options(ENTREZ_KEY = Sys.getenv("ENTREZ_KEY"))
if(!dir.exists("output")) dir.create("output")

# 动物 ----------------------------------------------------------------------

# 1. 载入内置数据集
data(inset_demo)

# 2. 三大数据库分别平行全量执行，网速决定运行等待的时间
res_ncbi_insect <- search_ncbi_Ani(inset_demo$input.name)
res_gbif_insect <- search_gbif_Ani(inset_demo$input.name)
res_bold_insect <- search_bold_Ani(inset_demo$input.name)

# 3. 合并 (不去重拼接，行数为原始物种数 x 3)
combined_animal <- bind_rows(res_ncbi_insect, res_gbif_insect, res_bold_insect)
writexl::write_xlsx(combined_animal, 'output/combined_animal_all_db.xlsx')

# 4. 流水式检索（自定义优先考虑的数据库和类群）
final_data <- run_taxa_pipeline(
  raw_names = inset_demo$input.name,
  group = "Animal",
  db_sequence = c("NCBI", "GBIF", "BOLD"), # 在这里可以自定义数据库的检索顺序和数量
  output_dir = "output",
  interactive = F
)


# 植物 ----------------------------------------------------------------------

# 1. 载入内置数据集
data(plant_demo)

# 2. 三大数据库分别平行全量执行，网速需要等待
res_ncbi_plant <- search_ncbi_Pla(plant_demo$input.name)
res_gbif_plant <- search_gbif_Pla(plant_demo$input.name)# gbif比较久
res_bold_plant <- search_bold_Pla(plant_demo$input.name)

# 3. 合并 (不去重拼接，行数为原始物种数 x 3)
combined_plant <- bind_rows(res_ncbi_plant, res_gbif_plant, res_bold_plant)
writexl::write_xlsx(combined_plant, 'output/combined_plant_all_db.xlsx')

# 4. 流水式检索（自定义优先考虑的数据库和类群）
final_data <- run_taxa_pipeline(
  raw_names = plant_demo$input.name,
  group = "Plant",
  db_sequence = c("BOLD", "NCBI", "GBIF"), # 在这里自定义顺序和数量
  output_dir = "output",
  interactive = T
)

