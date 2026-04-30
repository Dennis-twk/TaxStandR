#' BOLD 数据库动物/昆虫物种全量检索与清洗 (BOLD Database Species Search for Animals)
#'
#' @description
#' 本函数专为 BOLD (Barcode of Life Data System) 数据库设计，专门用于检索植物 (Plantae) 数据。
#' 具备自动清理乱码、严格匹配、以及人工交互盲查复核功能。
#'
#' 针对 BOLD 数据库的特殊性，本模块内置了以下专属机制：
#' 1. 动态识别不稳定的返回列名（taxon / tax / name）；
#' 2. 强制为缺失的界 (Kingdom) 填补 "Animalia"；
#' 3. 跨库联动：提取 BOLD 查到的科名 (Family)，跨库调用 NCBI 接口补齐总科 (Superfamily) 信息。
#'
#' @param raw_names 字符型向量 (Character vector)。包含需要清洗和查询的原始动物物种名称。
#'
#' @return 返回一个数据框 (Data.frame)。包含标准化后的物种名、BOLD 数据库的 taxid、
#' 完整的 8 级分类阶元信息（包含从 NCBI 跨库补齐的 superfamily），以及追踪数据来源的 `source` 列。
#'
#' @export
#' @importFrom taxize get_gbifid get_gbifid_
#' @import dplyr
#' @importFrom writexl write_xlsx
#' @importFrom stringr str_remove


search_bold_Ani <- function(raw_names) {
  cat("\n 开始执行 BOLD 数据库检索 (昆虫) \n")
  # 移除可能存在的 XML 残留标签
  raw_names <- str_remove(raw_names, '^xml:space="preserve">')
  # 调用本包的预处理函数
  df_map <- data.frame(input.name = unique(raw_names), stringsAsFactors = FALSE)
  df_map$preprocessed.name <- preprocess_species_names(df_map$input.name)
  search_list <- unique(df_map$preprocessed.name)

  # --- 第一轮：严格匹配 ---
  pb <- txtProgressBar(min = 0, max = length(search_list), style = 3)
  ids_list <- lapply(seq_along(search_list), function(i) {
    # 屏蔽内部警告，保持控制台整洁，调用自动重试机制获取 BOLD ID
    res <- auto_retry({ suppressWarnings(suppressMessages(taxize::get_boldid(search_list[i], ask = FALSE, messages = FALSE))) })
    setTxtProgressBar(pb, i); return(res)
  })
  close(pb); cat("\n")
  # 整理第一轮结果，把提取出的 ID 放进数据框
  out_id <- data.frame(
    preprocessed.name = search_list,
    taxid = sapply(ids_list, function(x) if(is.null(x) || is.na(x[1])) NA_character_ else as.character(x[1])),
    stringsAsFactors = FALSE
  )
  # 分离出没查到的物种，准备进入第二轮深度挖掘
  df_missing <- out_id %>% filter(is.na(taxid))
  df_auto_recovered <- data.frame(preprocessed.name=character(), taxid=character(), stringsAsFactors=FALSE)
  df_manual_pass <- data.frame(preprocessed.name=character(), taxid=character(), stringsAsFactors=FALSE)

  # --- 第二轮：深度检索与自动/人工复核 ---
  if(nrow(df_missing) > 0) {
    cat(sprintf("【 BOLD 】 有 %d 个物种需深度匹配（如：基于词尾等效与前缀匹配）。正在检索候选列表...\n", nrow(df_missing)))
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
          # 剔除名称为 NA 的无效行
          res <- res[!is.na(res[[name_col]]), , drop = FALSE]

          if(nrow(res) > 0) {
            sp_norm <- norm_name(sp)
            cand_names <- res[[name_col]]

            match_cond <- (norm_name(cand_names) == sp_norm | startsWith(norm_name(cand_names), paste0(sp_norm, " ")))
            # 处理潜在的 NA，防止逻辑报错
            match_cond[is.na(match_cond)] <- FALSE

            exact_matches <- res[match_cond, , drop = FALSE]

            if(nrow(exact_matches) > 0) {
              best_id <- exact_matches$taxid[1]
              df_auto_recovered <- rbind(df_auto_recovered, data.frame(preprocessed.name = sp, taxid = as.character(best_id), stringsAsFactors = FALSE))
              auto_picked <- TRUE
            }

            # 如果自动没匹配上，依然有候选数据，放入人工复核
            if(!auto_picked) {
              cands_list[[sp]] <- head(res, 3)
            }
          }
        }
      }
      setTxtProgressBar(pb_man, i)
    }
    close(pb_man)

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
          # 提取当前需要显示的信息
          name_str <- get_v(k, c("taxon", "tax", "name")) # BOLD 里的 taxon 字段，默认就是非常干净的纯拉丁名（不带命名人和年份）
          rank_str <- toupper(get_v(k, "tax_rank"))
          div_str <- get_v(k, "tax_division")

          # 使用占位符强制对齐
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

    # 合并自动挽回和人工通过的数据
    recovered_all <- bind_rows(df_auto_recovered, df_manual_pass)
    if(nrow(recovered_all) > 0) {
      out_id <- out_id %>% filter(!preprocessed.name %in% recovered_all$preprocessed.name) %>% bind_rows(recovered_all)
    }
  } else {
    cat("【 BOLD 】 全部物种一次性自动匹配成功，无需人工复核！\n")
  }

  # --- 第三轮：最终处理分类信息等流程 ---
  df_pass <- out_id %>% filter(!is.na(taxid))

  # 初始化空底表防报错
  res_parsed <- data.frame(preprocessed.name = character(), taxid = character(), stringsAsFactors = FALSE)

  if(nrow(df_pass) > 0) {
    # 提取 BOLD 分类信息
    classf <- safe_classification(df_pass$taxid, 'bold')
    parsed <- parse_taxonomy_Ani(classf, db_name = "BOLD")
    res_parsed <- df_pass %>% left_join(parsed, by = "taxid")

    # BOLD 专属补丁：强制填补界（Kingdom = Animalia）
    if(!"BOLD.kingdom" %in% names(res_parsed)) res_parsed$BOLD.kingdom <- NA_character_
    res_parsed$BOLD.kingdom[is.na(res_parsed$BOLD.kingdom)] <- "Animalia"

    # 跨库调用 NCBI 补齐总科（Superfamily）
    if(!"BOLD.family" %in% names(res_parsed)) res_parsed$BOLD.family <- NA_character_
    cat("  => 正在匹配总科(Superfamily)...\n")
    fam_to_super <- get_ncbi_superfamily_Ani(res_parsed$BOLD.family)
    res_parsed <- res_parsed %>% left_join(fam_to_super, by = c("BOLD.family" = "family"))
  }

  # 防 0 行数据导致报错的安全补列机制
  expected_cols <- c("BOLD.kingdom", "BOLD.phylum", "BOLD.class", "BOLD.order",
                     "BOLD.family", "BOLD.genus", "BOLD.species", "NCBI.superfamily")

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
              superfamily = NCBI.superfamily, family = BOLD.family, genus = BOLD.genus, species = BOLD.species, source)
  cat("  => 已通过【BOLD】完成包含总科在内的分类信息的检索！\n")

  save_path <- 'output/BOLD_results_Ani.xlsx'
  tryCatch({
    writexl::write_xlsx(res_final, save_path)
    cat(sprintf("【 BOLD 完成 】 单库记录已成功保存至: %s\n", save_path))

  }, error = function(e) {
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    fallback_path <- sprintf('output/BOLD_results_Ani_backup_%s.xlsx', timestamp)
    writexl::write_xlsx(res_final, fallback_path)
    cat("\n====================== 【 警告 】 ======================\n")
    cat("检测到目标 Excel 文件正在被打开（如被锁定），无法覆盖！\n")
    cat(sprintf("为了防止数据丢失，结果已自动另存为备用文件:\n  --> %s\n", fallback_path))
    cat("========================================================\n")
  })

  return(res_final)
}
