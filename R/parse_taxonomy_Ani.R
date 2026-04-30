#' 解析动物分类树列表为宽数据框 (Parse Animal Taxonomy List)
#'
#' @description
#' 本函数用于将 `taxize::classification()` 或自定义函数 `safe_classification()`
#' 返回的复杂嵌套列表（List）扁平化，转换为宽格式（Wide format）的数据框（Data.frame）。
#' 专为动物/昆虫设计，提取并对齐 8 个核心分类阶元：
#' 界 (kingdom), 门 (phylum), 纲 (class), 目 (order), 总科 (superfamily), 科 (family), 属 (genus), 种 (species)。
#'
#' @param class_obj 列表 (List)。由分类检索提取函数返回的分类阶元列表，列表的名称（names）必须为对应的 taxid。
#' @param db_name 字符型 (Character)。指定数据库的名称前缀，默认为 `"NCBI"`（支持 `"GBIF"`, `"BOLD"` 等）。
#' 生成的数据框列名将以此作为前缀（例如：`NCBI.kingdom`, `GBIF.family`）。
#'
#' @return 返回一个数据框 (Data.frame)。包含 `taxid` 列，以及带有数据库前缀的 8 个分类阶元列。
#' 若某个物种缺失某一层级的阶元，对应的值将自动填充为 `NA`。
#'
#' @export
#'
#' @importFrom purrr imap_dfr
#' @import dplyr
#' @importFrom tidyr pivot_wider
#' @importFrom stats setNames
#'
parse_taxonomy_Ani <- function(class_obj, db_name = "NCBI") { # 参数赋默认值：默认来源于NCBI
  target_ranks <- c("kingdom", "phylum", "class", "order", "superfamily", "family", "genus", "species")
  parsed_df <- imap_dfr(class_obj, function(df, taxid) { # 自动遍历由 taxize 返回的分类信息列表
    # 错误与空值拦截
    if (is.null(df) || (length(df) == 1 && is.na(df)))
      return(data.frame(taxid = taxid, stringsAsFactors = FALSE))
    # 数据长宽转换
    df_wide <- df %>% select(name, rank) %>% filter(rank %in% target_ranks) %>%
      distinct(rank, .keep_all = TRUE) %>% pivot_wider(names_from = rank, values_from = name)
    # 强制列名对齐
    for (col in target_ranks) if (!col %in% names(df_wide)) df_wide[[col]] <- NA
    df_wide$taxid <- taxid
    return(df_wide) # 贴上它对应的身份证号（taxid）
  })
  # 贴上数据库前缀标签
  rename_mapping <- setNames(target_ranks, paste0(db_name, ".", target_ranks))
  return(parsed_df %>% rename(any_of(rename_mapping)))
}
