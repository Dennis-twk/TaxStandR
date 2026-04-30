#' 网络请求自动重试 (Automatic Retry for API Requests)
#'
#' @description
#' 针对不稳定网络或 API 速率限制（Rate Limits）的防御性函数。
#' 当向 NCBI、GBIF 或 BOLD 等外部数据库发送请求时，经常会因为网络波动或请求过频而报错断开。
#' 本函数通过 R 语言的元编程（Metaprogramming）技术截获代码块，一旦发生报错不会使程序崩溃，
#' 而是进入休眠并自动重试，直到达到最大重试次数后返回 `NULL`，保障大规模数据流水线平稳运行。
#'
#' @param code_block 表达式 (Expression)。需要被保护和执行的任意 R 代码块（例如 `taxize::get_uid(...)`）。
#' @param max_attempts 整型 (Integer)。最大允许尝试的次数，默认值为 4 次。
#' @param sleep_sec 数值型 (Numeric)。每次失败后，程序强制休眠等待的秒数，默认值为 2 秒。
#'
#' @return 如果执行成功，将返回 `code_block` 代码块本身原本应该返回的结果。
#' 如果达到最大重试次数依然失败，则返回 `NULL`。
#'
#' @export
#'


auto_retry <- function(code_block, max_attempts = 4, sleep_sec = 2) {
  expr <- substitute(code_block); env <- parent.frame()
  for (i in 1:max_attempts) {
    res <- tryCatch({ eval(expr, envir = env) }, error = function(e) e)
    if (!inherits(res, "error"))
      return(res)
    if (i < max_attempts) {
      cat(sprintf("\n【警告】 API 请求报错: %s。将在 %d 秒后进行重试...\n", conditionMessage(res), sleep_sec))
      Sys.sleep(sleep_sec) # Sys.sleep 强制让 R 程序进入休眠（挂起）状态。
    } else { # 连试了 4 次（默认）全都失败了打印错误提示，放弃这一个物种，防止整个程序死循环卡死。
      cat(sprintf("\n【错误】 已达到最大重试次数，跳过当前项。\n"))
    }
  }
  return(NULL)
}

