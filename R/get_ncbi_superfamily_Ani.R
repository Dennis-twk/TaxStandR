#' 批量获取动物科级（Family）对应的 NCBI 总科（Superfamily）信息
#'
#'#' @description
#' 专为动物/昆虫数据集的“总科补齐”模块。
#' 由于 GBIF 和 BOLD 数据库的分类树通常缺失“总科 (superfamily)”这一分类阶元，
#' 该函数通过提取数据中已查到的“科 (family)”名，前往 NCBI 数据库进行批量反向查询，
#' 拉取完整的分类树，并剥离出对应的 NCBI 总科名称。
#'
#' @param family_vector 字符型向量 (Character vector)。包含需要查询的“科”名组合（例如 `c("Noctuidae", "Geometridae")`）。
#'
#' @return 返回一个数据框 (Data.frame)。必定包含两列：
#' `family`（输入的科名）和 `NCBI.superfamily`（在 NCBI 中查到的总科名）。
#' 如果某个科名在 NCBI 中未查到，对应的总科列将填充为 `NA`。
#'
#' @export
#'
#' @importFrom taxize get_uid
#' @importFrom utils capture.output
#' @import dplyr
#'
get_ncbi_superfamily_Ani <- function(family_vector) {
  family_vector <- unique(family_vector[!is.na(family_vector)]) # 剔除输入向量中的 NA 值，并对科名进行去重，减少不必要的重复 API 请求
  # 防崩溃保护：如果过滤后发现全是 NA 或输入为空，直接返回标准的空数据框，避免后续报错
  if(length(family_vector) == 0)
    return(data.frame(family = character(), NCBI.superfamily = character()))
  # capture.output() 用于拦截底层 taxize 包产生的冗余打印输出，保持控制台整洁
  # 调用自定义的 auto_retry，静默地 (ask=FALSE, messages=FALSE) 批量获取这些科名的 NCBI UID
  capture.output({ ids_superfamily <- auto_retry({ suppressWarnings(
    suppressMessages(taxize::get_uid(family_vector, ask = FALSE, messages = FALSE))) }) })
  # 网络崩溃或API失效时的保底机制：将返回结果强行转换为等长的 NA 向量，保证数据结构不乱
  if(is.null(ids_superfamily))
    ids_superfamily <- rep(NA, length(family_vector))
  # 构建“科名-ID”的映射关系表
  out_id <- data.frame(family = family_vector, ncbi.taxid = as.character(ids_superfamily)) %>%
    filter(!is.na(ncbi.taxid))
  # 提取分类信息并剥离出“总科”
  if(nrow(out_id) > 0) {
    classf <- safe_classification(out_id$ncbi.taxid, 'ncbi') # 调用自定义安全函数批量拉取完整的 NCBI 分类树信息 (List 格式)
    parsed <- parse_taxonomy_Ani(classf, db_name = "NCBI") # 调用自定义解析函数，把嵌套的分类树列表扁平化，转换为带有 "NCBI." 前缀的宽数据框
    # 将刚生成的映射表 (UID) 与解析好的分类宽表 (含总科) 进行左连接合并，只选取我们需要的 "family" 和 "NCBI.superfamily" 两列
    return(out_id %>% left_join(parsed, by = c("ncbi.taxid" = "taxid")) %>% select(family, NCBI.superfamily))
  } else {
    return(data.frame(family = family_vector, NCBI.superfamily = NA_character_)) } # 若没有UID则总科全部填为 NA并输出原始科名
}
