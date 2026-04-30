#' NCBI 数据库植物物种全量检索与清洗 (NCBI Database Species Search for Plants)
#'
#' @description
#' 本函数专为 NCBI (National Center for Biotechnology Information) Taxonomy 数据库设计，
#' 专门用于检索植物 (Plantae) 数据。作为植物流水线的第一道核心关卡，它具备极高的查准率。
#'
#' 专属核心机制：
#' 1. **全自动乱码清洗**：内置 XML 残留剥离与拉丁学名提纯功能；
#' 2. **拉丁文法容错引擎**：内置 4 大拉丁形容词变格映射（如 -us/-um 映射 -a，-is 映射 -e），
#'    无缝抹平跨属变名导致的拼写差异，极大提高 NCBI 对植物异名的自动挽回率；
#' 3. **植物专属降维**：精准提取植物核心的 7 级分类阶元（去除了总科），极大提高运算效率；
#' 4. **零行防御机制**：哪怕全盘匹配失败，也能安全输出包含所有列名的标准空表，捍卫流水线稳定。
#'
#' @param raw_names 字符型向量 (Character vector)。包含需要清洗和查询的原始植物物种名称。
#'
#' @return 返回一个数据框 (Data.frame)。包含标准化后的物种名、NCBI 数据库的 taxid、
#' 完整的 7 级植物分类阶元树，以及追踪数据来源的 `source` 列。
#' @export
#' @importFrom taxize get_gbifid get_gbifid_
#' @import dplyr
#' @importFrom writexl write_xlsx
#'
search_ncbi_Pla <- function(raw_names) {

  cat("\n 开始执行 NCBI 数据库检索 (植物) \n")
  # 移除可能存在的 XML 残留标签
  raw_names <- str_remove(raw_names, '^xml:space="preserve">')

  # 调用预处理函数
  df_map <- data.frame(input.name = unique(raw_names), stringsAsFactors = FALSE)
  df_map$preprocessed.name <- preprocess_species_names(df_map$input.name)
  search_list <- unique(df_map$preprocessed.name) # 移除重复的物种名

  # --- 第一轮：自动严格检索 ---
  pb <- txtProgressBar(min = 0, max = length(search_list), style = 3)
  ids_list <- lapply(seq_along(search_list), function(i) {
    res <- auto_retry({ suppressWarnings(suppressMessages(taxize::get_uid(search_list[i], ask = FALSE, messages = FALSE))) })
    setTxtProgressBar(pb, i); return(res)
  })
  close(pb); cat("\n")

  out_id <- data.frame(
    preprocessed.name = search_list,
    taxid = sapply(ids_list, function(x) if(is.null(x) || is.na(x[1])) NA_character_ else as.character(x[1])),
    stringsAsFactors = FALSE
  )

  df_missing <- out_id %>% filter(is.na(taxid))

  df_auto_recovered <- data.frame(preprocessed.name=character(), taxid=character(), stringsAsFactors=FALSE)
  df_manual_pass <- data.frame(preprocessed.name=character(), taxid=character(), stringsAsFactors=FALSE)

  # --- 第二轮：深度检索与自动/人工复核 ---
  if(nrow(df_missing) > 0) {
    cat(sprintf("【 NCBI 】 有 %d 个物种需深度匹配（如：基于词尾等效与前缀匹配）。正在检索候选列表...\n", nrow(df_missing)))
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
      res <- auto_retry({ suppressWarnings(suppressMessages(taxize::get_uid_(sp, messages = FALSE)[[1]])) })

      auto_picked <- FALSE
      if(is.data.frame(res) && nrow(res) > 0) {
        names(res) <- tolower(names(res))

        # ================= NCBI 优化的自动智能筛选逻辑 =================
        if("scientificname" %in% names(res)) {
          # 使用 drop = FALSE 防御性保留数据框结构
          res <- res[!is.na(res$scientificname), , drop = FALSE]

          if(nrow(res) > 0) {
            sp_norm <- norm_name(sp)
            cand_names <- res$scientificname

            match_cond <- (norm_name(cand_names) == sp_norm | startsWith(norm_name(cand_names), paste0(sp_norm, " ")))
            match_cond[is.na(match_cond)] <- FALSE # 处理潜在的 NA，防止逻辑报错

            exact_matches <- res[match_cond, , drop = FALSE]

            if(nrow(exact_matches) > 0) {
              # 提取 NCBI 的唯一标识符 uid
              best_id <- exact_matches$uid[1]
              df_auto_recovered <- rbind(df_auto_recovered, data.frame(preprocessed.name = sp, taxid = as.character(best_id), stringsAsFactors = FALSE))
              auto_picked <- TRUE
            }
          }
        }

        if(!auto_picked) {
          cands_list[[sp]] <- head(res, 3)
        }
      }
      setTxtProgressBar(pb_man, i)
    }
    close(pb_man)
    # 剩下真正需要人工介入的打印
    if(length(cands_list) > 0) {
      cat(sprintf("\n【NCBI 人工复核】 仍有 %d 个物种存在歧义或拼写差异，需人工确认：\n", length(cands_list)))
      c_name <- c(); c_id <- c()
      for(sp in names(cands_list)) {
        df <- cands_list[[sp]]; cat(sprintf("\n--- 【NCBI 人工】 待选物种: %s ---\n", sp))
        names(df) <- tolower(names(df))

        # 安全提取列值的辅助函数
        get_v <- function(r, cols) {
          for(c in cols) if(c %in% names(df) && !is.na(df[[c]][r])) return(as.character(df[[c]][r]))
          return("NA")
        }

        for(k in 1:nrow(df)) {
          name_str <- get_v(k, "scientificname") # NCBI 数据库里的 scientificname 默认就是极其干净的纯拉丁名（即基础学名）
          rank_str <- toupper(get_v(k, "rank"))
          div_str <- get_v(k, "division")

          cat(sprintf(" [%d] %-30s (级别: %-8s | 分区: %s)\n",
                      k, name_str, rank_str, div_str))
        }
        cat(" [0] 均不匹配，跳过\n")
        ans_num <- -1
        while(!(ans_num %in% 0:nrow(df))) { ans_num <- suppressWarnings(as.integer(trimws(readline(prompt = "请输入序号: ")))) }
        if(ans_num > 0) { c_name <- c(c_name, sp); c_id <- c(c_id, get_v(ans_num, "uid")) }
      }
      if(length(c_id) > 0) {
        df_manual_pass <- data.frame(preprocessed.name = c_name, taxid = c_id, stringsAsFactors = FALSE)
      }
    }

    recovered_all <- bind_rows(df_auto_recovered, df_manual_pass)
    if(nrow(recovered_all) > 0) {
      out_id <- out_id %>% filter(!preprocessed.name %in% recovered_all$preprocessed.name) %>% bind_rows(recovered_all)
    }
  } else {
    cat("【 NCBI 】 全部物种一次性自动匹配成功，无需人工复核！\n")
  }

  # --- 第三轮：提取分类信息 (植物专用) ---
  df_pass <- out_id %>% filter(!is.na(taxid))

  # 初始化空底表防0行数据崩溃
  res_parsed <- data.frame(preprocessed.name = character(), taxid = character(), stringsAsFactors = FALSE)

  if(nrow(df_pass) > 0) {
    classf <- safe_classification(df_pass$taxid, 'ncbi')
    parsed <- parse_taxonomy_Pla(classf, db_name = "NCBI")
    res_parsed <- df_pass %>% left_join(parsed, by = "taxid")
  }

  # 安全补齐 7 级植物阶元
  expected_cols <- c("NCBI.kingdom", "NCBI.phylum", "NCBI.class", "NCBI.order", "NCBI.family", "NCBI.genus", "NCBI.species")
  for(col in expected_cols) {
    if(!col %in% names(res_parsed)) {
      res_parsed[[col]] <- if(nrow(res_parsed) == 0) character(0) else NA_character_
    }
  }

  # --- 第四轮：组装最终输出表格 (注意：植物版无总科 superfamily) ---
  res_final <- df_map %>% left_join(res_parsed, by = "preprocessed.name") %>%
    mutate(source = case_when(
      is.na(taxid) ~ "NCBI_Not_Found",
      preprocessed.name %in% df_auto_recovered$preprocessed.name ~ "NCBI_Auto_Recovered",
      preprocessed.name %in% df_manual_pass$preprocessed.name ~ "NCBI_Manual",
      TRUE ~ "NCBI_Strict"
    )) %>%
    transmute(input.name, cleaned.name = preprocessed.name, taxid,
              kingdom = NCBI.kingdom, phylum = NCBI.phylum, class = NCBI.class, order = NCBI.order,
              family = NCBI.family, genus = NCBI.genus, species = NCBI.species, source)
  cat("  => 已通过【NCBI】完成所有分类信息的检索！\n")

  save_path <- 'output/NCBI_results_Pla.xlsx'

  tryCatch({
    writexl::write_xlsx(res_final, save_path)
    cat(sprintf("【 NCBI 完成 】 单库记录已成功保存至: %s\n", save_path))

  }, error = function(e) {
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    fallback_path <- sprintf('output/NCBI_results_Pla_backup_%s.xlsx', timestamp)

    writexl::write_xlsx(res_final, fallback_path)

    cat("\n====================== 【 警告 】 ======================\n")
    cat("检测到目标 Excel 文件正在被打开（如被锁定），无法覆盖！\n")
    cat(sprintf("为了防止数据丢失，结果已自动另存为备用文件:\n  --> %s\n", fallback_path))
    cat("========================================================\n")
  })

  return(res_final)
}
