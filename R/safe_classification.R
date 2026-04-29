#' 安全获取物种分类树信息 (Safely Retrieve Classification Tree)
#'
#' @description
#' 这是 `TaxStandR` 包中负责提取物种完整分类树（门、纲、目、科、属、种等）的核心网络请求模块。
#' 本函数是对底层 `taxize::classification()` 的安全封装（Wrapper）。
#'
#' 它具备以下特性：
#' 1. 自动去重与清洗：自动过滤掉 `NA` 及无效的 `"NA"` 字符串，并剔除重复 ID，节省网络请求时间。
#' 2. 防崩溃网络重试：无缝接入本包的 `auto_retry()`，即使遭遇网络中断也能自动重试。
#' 3. UI 进度反馈：内置控制台进度条（Progress bar），在处理成千上万条数据时提供直观的耗时预期。
#' 4. 严苛的返回值校验：严格审查 API 的返回格式，只有返回标准数据框（Data frame）才会被接纳，
#'    否则一律安全转化为 `NA`，彻底杜绝畸形数据导致的后续合并崩溃。
#'
#' @param taxids 字符型或数值型向量 (Character/Numeric vector)。需要查询的物种分类 ID (Taxon IDs)。
#' @param db 字符型 (Character)。目标数据库名称，支持 `"ncbi"`, `"gbif"`, 或 `"bold"`。
#'
#' @return 返回一个命名列表 (Named list)。列表的名称（names）即为传入的 `taxids`。
#' 如果对应 ID 查询成功，列表元素将是一个包含各级分类阶元的数据框 (Data frame)；
#' 如果查询失败或无数据，该元素将安全地赋值为 `NA`。
#'
#' @export
#'
#' @importFrom taxize classification
#'

safe_classification <- function(taxids, db) {
  taxids <- unique(taxids[!is.na(taxids) & taxids != "NA"])
  res_list <- list()
  if(length(taxids) == 0) return(res_list)
  # 进度条初始化
  cat(sprintf("  => 正在获取分类信息（共 %d 项）...\n", length(taxids)))
  pb <- txtProgressBar(min = 0, max = length(taxids), style = 3)
  # 遍历每个ID并逐一向数据库发送请求
  for(j in seq_along(taxids)) {
    tid <- taxids[j]
    # 调用自定义的 auto_retry 自动重试函数，并屏蔽 API 的冗长警告以保持进度条整洁
    c_res <- auto_retry({ suppressWarnings(
      suppressMessages(taxize::classification(tid, db = db))) })
    # 返回的结果 c_res 必须通过三项严格检查：非 NULL、长度大于 0，且为数据框
    if(!is.null(c_res) && length(c_res) > 0 && is.data.frame(c_res[[1]])) {
      res_list[[tid]] <- c_res[[1]]
    } else {
      res_list[[tid]] <- NA
    }
    # 刷新进度条
    setTxtProgressBar(pb, j)
  }
  close(pb); cat("\n")
  return(res_list)
}
