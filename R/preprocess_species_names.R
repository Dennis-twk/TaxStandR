#' 物种学名标准化清洗与预处理 (Preprocess Species Names for Matching)
#'
#' @description
#' 本函数是 `TaxStandR` 包的文本预处理模块。专为处理生态学调查、
#' 文献挖掘或开放数据库中极其不规范的原始物种学名（Raw Species Names）而设计。
#' 它通过一条严密的正则表达式（Regex）流水线，依次完成以下清洗任务：
#' 1. 移除杂交符号（×, x, X）；
#' 2. 去除拼写重音符号及特殊乱码；
#' 3. 剥离括号内的命名人及年份信息；
#' 4. 截断亚种/变种（var., f., subsp.）至种级，降级存疑种（cf., sp.）至属级；
#' 5. 剔除无括号保护的末尾大写命名人、标点及纯数字；
#' 6. 提取标准的“属名 + 种加词”二名法格式，并统一为句首字母大写（Sentence case）。
#'
#' @param names 字符型向量 (Character vector)。包含需要清洗的原始物种名称。
#'
#' @return 返回一个经过清洗、标准化的字符型向量 (Character vector)。
#' 格式统一为拉丁学名标准的二名法（如 `"Homo sapiens"`），去除了 names 属性。
#'
#' @export
#' @import stringr
#' @importFrom stringi stri_trans_general
#'

preprocess_species_names <- function(names) {
  # 优先处理杂交符号 (将 ×、x、X 前后的空格规范化，并最终移除该符号)
  names <- str_replace_all(names, "(?i)\\s+[\u00D7xX]\\s+", " ")
  names <- str_replace_all(names, "\u00D7", "")

  # 转换为基础 ASCII 字符，处理带音标的拉丁文（如 á, ë, ñ）和乱码字体
  names <- stri_trans_general(names, "Latin-ASCII")

  # 移除括号及其包含的内容（通常用于去除分类作者和命名年份）
  names <- str_replace_all(names, "\\(.*?\\)", "")

  # 亚种/变种处理（截断保留至种级别） / 存疑或未定种处理（降级保留至属级别）
  names <- str_replace(names, "(?i)\\s*\\b(var\\.|f\\.|subsp\\.|ssp\\.|nov\\.).*", "")
  names <- ifelse(str_detect(names, "(?i)\\b(cf\\.|aff\\.|nr\\.|pr\\.|sp\\.|spp\\.)"),
                  str_extract(names, "^\\s*[A-Za-z]+"), names)

  # 将下划线、连字符及其他常见标点符号替换为空格（防止由下划线相连的名字连成一坨）
  names <- str_replace_all(names, "[_,:?\\-]", " ")

  # 去掉首尾和多余的连续空格，确保字符串首字母就是有效内容的开始
  names <- str_squish(names)

  # 剔除非首字母的大写字母及其连带的后缀串（直到遇到空格）
  names <- str_replace_all(names, "(?<=.)[A-Z]\\S*", "")

  # 替换后可能会留下多余的空格，再次清理一下
  names <- str_squish(names)

  # 移除名称中的所有数字 (如果前面大写字母带的数字被清掉了，这里只会清掉剩余的纯数字)
  names <- str_replace_all(names, "[0-9]+", "")
  names <- str_squish(names) # 清除由于删除数字可能留下的多余空格

  # 提取物种学名的属名和种加词（即只保留前两个单词）
  names <- sapply(names, function(x) {
    parts <- unlist(str_split(x, "\\s+"))
    if(length(parts) >= 2) paste(parts[1], parts[2]) else x
  })

  # 统一转换为句首字母大写格式 (例如：Trichopterigia rubripuncta)
  return(unname(str_to_sentence(str_to_lower(names))))
}
