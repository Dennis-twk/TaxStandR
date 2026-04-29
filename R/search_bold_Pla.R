#' BOLD 数据库植物物种全量检索与清洗 (BOLD Database Species Search for Plants)
#'
#' @description
#' 本函数专为 BOLD (Barcode of Life Data System) 数据库设计，专门用于检索植物 (Plantae) 数据。
#' 具备自动清理乱码、严格匹配以及人工交互盲查复核功能。
#'
#' 针对植物在 BOLD 数据库中的特殊性，本模块内置了以下机制：
#' 1. 去除了植物分类学中较少使用的“总科 (superfamily)”阶元提取，加快运行效率；
#' 2. 强制为缺失的界 (Kingdom) 填补 "Plantae" (植物界)
#' 3. 预防空值出现报错：即使所有物种均未匹配成功，也能安全输出标准格式的空表，绝不使流水线崩溃。
#'
#' @param raw_names 字符型向量 (Character vector)。包含需要清洗和查询的原始植物物种名称。
#'
#' @return 返回一个数据框 (Data.frame)。包含标准化后的物种名、BOLD 数据库的 taxid、
#' 完整的 7 级植物分类阶元信息，以及追踪数据来源的 `source` 列。
#'
#' @export
#'
#' @importFrom taxize get_boldid get_boldid_
#' @import dplyr
#' @importFrom writexl write_xlsx
#' @importFrom stringr str_remove
#'

search_bold_Pla <- function(raw_names) {
  cat("\n 开始执行 BOLD 数据库检索 (植物) \n")
  # 移除可能存在的 XML 残留标签
  raw_names <- str_remove(raw_names, '^xml:space="preserve">')

  # 调用本包的核心预处理函数
  df_map <- data.frame(input.name = unique(raw_names), stringsAsFactors = FALSE)
  df_map$preprocessed.name <- preprocess_species_names(df_map$input.name)
  search_list <- unique(df_map$preprocessed.name)

  # --- 第一轮：严格匹配 ---
  pb <- txtProgressBar(min = 0, max = length(search_list), style = 3)
  ids_list <- lapply(seq_along(search_list), function(i) {
    res <- auto_retry({ suppressWarnings(suppressMessages(taxize::get_boldid(search_list[i], ask = FALSE, messages = FALSE))) })
    setTxtProgressBar(pb, i); return(res)
  })
  close(pb); cat("\n")

  out_id <- data.frame(
    preprocessed.name = search_list,
    taxid = sapply(ids_list, function(x) if(is.null(x) || is.na(x[1])) NA_character_ else as.character(x[1])),
    stringsAsFactors = FALSE
  )

  df_missing <- out_id %>% filter(is.na(taxid))

  # 初始化自动挽回与人工复核的数据框
  df_auto_recovered <- data.frame(preprocessed.name = character(), taxid = character(), stringsAsFactors = FALSE)
  df_manual_pass <- data.frame(preprocessed.name = character(), taxid = character(), stringsAsFactors = FALSE)

  # --- 第二轮：深度检索与自动/人工复核 ---
  if(nrow(df_missing) > 0) {
    cat(sprintf("\n【 BOLD 】 有 %d 个物种需深度匹配（如：基于词尾等效与前缀匹配）。正在检索候选列表...\n", nrow(df_missing)))
    pb_man <- txtProgressBar(min = 0, max = nrow(df_missing), style = 3)
    cands_list <- list()

    # 双向转换函数：
    norm_name <- function(name) {
      name <- tolower(trimws(name))

      # 常规形容词变格：阳性(-us) / 中性(-um) -> 统一映射为阴性(-a)
      name <- gsub("us\\b|um\\b", "a", name)

      # 第三变格形容词：阳/阴性(-is) -> 统一映射为中性(-e)
      name <- gsub("is\\b", "e", name)

      # 特殊形容词变格：阳性(-er) / 中性(-rum) -> 统一映射为阴性(-ra)
      name <- gsub("er\\b|rum\\b", "ra", name)

      # 命名人致敬后缀（Patronymics）：现代学者常把 -ii 简写为 -i，极易报错
      name <- gsub("ii\\b", "i", name)

      return(name)
    }

    for(i in 1:nrow(df_missing)) {
      sp <- df_missing$preprocessed.name[i]
      res <- auto_retry({ suppressWarnings(suppressMessages(taxize::get_boldid_(sp, messages = FALSE)[[1]])) })

      auto_picked <- FALSE
      if(is.data.frame(res) && nrow(res) > 0) {
        names(res) <- tolower(names(res))

        # 动态捕捉 BOLD 不稳定的列名
        name_col <- NULL
        if("taxon" %in% names(res)) name_col <- "taxon"
        else if("tax" %in% names(res)) name_col <- "tax"
        else if("name" %in% names(res)) name_col <- "name"

        if(!is.null(name_col)) {
          # 1. 剔除名称为 NA 的无效行
          res <- res[!is.na(res[[name_col]]), , drop = FALSE]

          # 2. 如果剔除后还有有效数据，进行智能匹配
          if(nrow(res) > 0) {
            sp_norm <- norm_name(sp)
            cand_names <- res[[name_col]]

            # 精确或前缀匹配判断
            match_cond <- (norm_name(cand_names) == sp_norm | startsWith(norm_name(cand_names), paste0(sp_norm, " ")))
            match_cond[is.na(match_cond)] <- FALSE

            exact_matches <- res[match_cond, , drop = FALSE]

            if(nrow(exact_matches) > 0) {
              best_id <- exact_matches$taxid[1]
              df_auto_recovered <- rbind(df_auto_recovered, data.frame(preprocessed.name = sp, taxid = as.character(best_id), stringsAsFactors = FALSE))
              auto_picked <- TRUE
            }

            # 如果自动没匹配上，依然有候选数据，放入人工复核候选池
            if(!auto_picked) {
              cands_list[[sp]] <- head(res, 3)
            }
          }
        }
      }
      setTxtProgressBar(pb_man, i)
    }
    close(pb_man)

    if(nrow(df_auto_recovered) > 0) {
      cat(sprintf("\n[ BOLD 自动找回 ] 已智能识别并跳过 %d 个物种！\n", nrow(df_auto_recovered)))
    }

    # 剩下真正需要人工介入的打印
    if(length(cands_list) > 0) {
      cat(sprintf("\n【BOLD 人工复核】 仍有 %d 个物种存在歧义或拼写差异，需人工确认：\n", length(cands_list)))
      c_name <- c(); c_id <- c()
      for(sp in names(cands_list)) {
        df <- cands_list[[sp]]; cat(sprintf("\n---  %s --- 【BOLD 人工】 待选:\n", sp))
        names(df) <- tolower(names(df))

        get_v <- function(r, cols) {
          for(c in cols) if(c %in% names(df) && !is.na(df[[c]][r])) return(as.character(df[[c]][r]))
          return("NA")
        }

        for(k in 1:nrow(df)) {
          name_str <- get_v(k, c("taxon", "tax", "name"))
          rank_str <- toupper(get_v(k, "tax_rank"))
          div_str <- get_v(k, "tax_division")

          cat(sprintf(" [%d] %-30s (级别: %-8s | 分区: %s)\n",
                      k, name_str, rank_str, div_str))
        }
        cat(" [0] 均不匹配，跳过\n")
        ans_num <- -1
        while(!(ans_num %in% 0:nrow(df))) { ans_num <- suppressWarnings(as.integer(trimws(readline(prompt = "请输入序号: ")))) }
        if(ans_num > 0) { c_name <- c(c_name, sp); c_id <- c(c_id, get_v(ans_num, "taxid")) }
      }
      if(length(c_id) > 0) {
        df_manual_pass <- data.frame(preprocessed.name = c_name, taxid = c_id, stringsAsFactors = FALSE)
      }
    }

    # 合并自动挽回和人工通过的数据，并更新回 out_id 底表
    recovered_all <- bind_rows(df_auto_recovered, df_manual_pass)
    if(nrow(recovered_all) > 0) {
      out_id <- out_id %>% filter(!preprocessed.name %in% recovered_all$preprocessed.name) %>% bind_rows(recovered_all)
    }
  } else {
    cat("\n【 BOLD 】 全部物种一次性自动匹配成功，无需人工复核！\n")
  }

  # --- 第三轮：最终处理分类信息等流程 ---

  df_pass <- out_id %>% filter(!is.na(taxid))

  # 初始化空底表防报错
  res_parsed <- data.frame(preprocessed.name = character(), taxid = character(), stringsAsFactors = FALSE)

  if(nrow(df_pass) > 0) {
    classf <- safe_classification(df_pass$taxid, 'bold')
    # 调用植物专属解析函数 (没有总科提取)
    parsed <- parse_taxonomy_Pla(classf, db_name = "BOLD")
    res_parsed <- df_pass %>% left_join(parsed, by = "taxid")

    # 植物专属核心补丁：强制补齐界 Kingdom = Plantae
    if(!"BOLD.kingdom" %in% names(res_parsed)) res_parsed$BOLD.kingdom <- NA_character_
    res_parsed$BOLD.kingdom[is.na(res_parsed$BOLD.kingdom)] <- "Plantae"
  }

  # 防止全部未匹配时找不到列而导致 transmute 崩溃
  expected_cols <- c("BOLD.kingdom", "BOLD.phylum", "BOLD.class", "BOLD.order",
                     "BOLD.family", "BOLD.genus", "BOLD.species")
  for(col in expected_cols) {
    if(!col %in% names(res_parsed)) {
      res_parsed[[col]] <- if(nrow(res_parsed) == 0) character(0) else NA_character_
    }
  }

  # 组装最终输出表格
  res_final <- df_map %>% left_join(res_parsed, by = "preprocessed.name") %>%
    mutate(source = case_when(
      is.na(taxid) ~ "BOLD_Not_Found",
      preprocessed.name %in% df_auto_recovered$preprocessed.name ~ "BOLD_Auto_Recovered",
      preprocessed.name %in% df_manual_pass$preprocessed.name ~ "BOLD_Manual",
      TRUE ~ "BOLD_Strict"
    )) %>%
    transmute(input.name, cleaned.name = preprocessed.name, taxid,
              kingdom = BOLD.kingdom, phylum = BOLD.phylum, class = BOLD.class, order = BOLD.order,
              family = BOLD.family, genus = BOLD.genus, species = BOLD.species, source)

  cat("  => 已通过【BOLD】完成植物分类信息提取的检索！\n")

  # # 加入文件防占用保护机制
  # save_path <- 'output/BOLD_full_results_Pla.xlsx'
  # tryCatch({
  #   writexl::write_xlsx(res_final, save_path)
  #   cat(sprintf("\n【 BOLD 完成 】 单库记录已成功保存至: %s\n", save_path))
  #
  # }, error = function(e) {
  #   timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  #   fallback_path <- sprintf('output/BOLD_full_results_Pla_backup_%s.xlsx', timestamp)
  #   writexl::write_xlsx(res_final, fallback_path)
  #   cat("\n====================== [ 警告 ] ======================\n")
  #   cat("检测到目标 Excel 文件正在被打开（如被锁定），无法覆盖！\n")
  #   cat(sprintf("为了防止数据丢失，结果已自动另存为备用文件:\n  --> %s\n", fallback_path))
  #   cat("========================================================\n")
  # })

  return(res_final)
}
