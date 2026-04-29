#' 核心物种清洗与全自动/半自动一体化瀑布流引擎 (Unified Taxa Pipeline)
#'
#' @description
#' 高度内聚的分类学处理引擎。无需外部依赖多个分库检索函数，内部集成了数据清洗、严格匹配、
#' 基于拉丁文术语的智能挽回、动态分类阶元信息解析以及防崩溃保护。
#'
#' @param raw_names 字符型向量。原始物种名称。
#' @param group 字符型。指定 "Animal" (提取8级含总科) 或 "Plant" (提取7级)。
#' @param db_sequence 字符型向量。数据库检索顺序，默认 c("NCBI", "GBIF", "BOLD")。
#' @param output_dir 字符型。结果保存路径，默认 "output"。
#' @param interactive 逻辑值。T/TRUE 表示开启人工交互复核；F/FALSE 表示极速全自动，默认 "F/FALSE"。
#'
#' @return 隐式返回合并后的结果总表，并在本地安全生成 Excel。
#' @export
#'
#' @import dplyr
#' @import stringr
#' @importFrom writexl write_xlsx
#' @importFrom taxize get_uid get_uid_ get_gbifid get_gbifid_ get_boldid get_boldid_
#'
run_taxa_pipeline <- function(raw_names,
                              group = c("Animal", "Plant"),
                              db_sequence = c("NCBI", "GBIF", "BOLD"),
                              output_dir = "output",
                              interactive = FALSE) {

  # ===================== 1. 参数校验与目录初始化 =====================
  group <- match.arg(group)
  db_sequence <- toupper(trimws(db_sequence))
  valid_dbs <- c("NCBI", "GBIF", "BOLD")
  db_sequence <- db_sequence[db_sequence %in% valid_dbs]
  if (length(db_sequence) == 0) db_sequence <- c("NCBI", "GBIF", "BOLD")

  if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  cat(sprintf("\n=== 启动 TaxStandR 一体化流水线 (%s) ===\n", group))
  cat(sprintf("\n检索顺序: %s | 人工交互: %s\n", paste(db_sequence, collapse = " -> "), ifelse(interactive, "开启", "关闭")))

  # ===================== 2. 全局数据清洗与预处理 =====================
  cat("=> 正在执行全局数据预清洗...\n")
  raw_names <- as.character(unlist(raw_names))
  raw_names_clean <- str_remove(raw_names, '^xml:space="preserve">')

  df_not_found <- data.frame(input.name = unique(raw_names_clean), stringsAsFactors = FALSE)
  df_not_found$preprocessed.name <- preprocess_species_names(df_not_found$input.name)

  final_results_list <- list()

  # 拉丁文法匹配
  norm_name <- function(name) {
    name <- tolower(trimws(name))
    name <- gsub("us\\b|um\\b", "a", name)
    name <- gsub("is\\b", "e", name)
    name <- gsub("er\\b|rum\\b", "ra", name)
    name <- gsub("ii\\b", "i", name)
    return(name)
  }

  # ===================== 3. 一体化瀑布流检索引擎 =====================
  for(db in db_sequence) {
    if(nrow(df_not_found) == 0) {
      cat(sprintf("\n所有物种在到达 %s 之前已全部匹配完毕！\n", db))
      break
    }

    cat(sprintf("\n>>>>>>>>>> 当前关卡: %s (待处理: %d 项) <<<<<<<<<<\n", db, nrow(df_not_found)))
    search_list <- unique(df_not_found$preprocessed.name)

    # ---------------- 3.1 第一轮：自动严格检索 ----------------
    pb <- txtProgressBar(min = 0, max = length(search_list), style = 3)
    ids_list <- lapply(seq_along(search_list), function(i) {
      sp <- search_list[i]
      res <- auto_retry({
        suppressWarnings(suppressMessages(
          switch(db,
                 "NCBI" = taxize::get_uid(sp, ask = FALSE, messages = FALSE),
                 "GBIF" = taxize::get_gbifid(sp, ask = FALSE, messages = FALSE),
                 "BOLD" = taxize::get_boldid(sp, ask = FALSE, messages = FALSE)
          )
        ))
      })
      setTxtProgressBar(pb, i); return(res)
    })
    close(pb)

    out_id <- data.frame(
      preprocessed.name = search_list,
      taxid = sapply(ids_list, function(x) if(is.null(x) || is.na(x[1])) NA_character_ else as.character(x[1])),
      stringsAsFactors = FALSE
    )

    df_missing <- out_id %>% filter(is.na(taxid))
    df_auto_recovered <- data.frame(preprocessed.name=character(), taxid=character(), stringsAsFactors=FALSE)
    df_manual_pass <- data.frame(preprocessed.name=character(), taxid=character(), stringsAsFactors=FALSE)

    # ---------------- 3.2 第二轮：深度检索与智能筛选/人工复核 ----------------
    if(nrow(df_missing) > 0) {
      cat(sprintf("  => 有 %d 个物种需深度匹配（如：基于词尾等效与前缀匹配）。正在检索候选列表...\n", nrow(df_missing)))
      pb_man <- txtProgressBar(min = 0, max = nrow(df_missing), style = 3)
      cands_list <- list()

      for(i in 1:nrow(df_missing)) {
        sp <- df_missing$preprocessed.name[i]
        res <- auto_retry({
          suppressWarnings(suppressMessages(
            switch(db,
                   "NCBI" = taxize::get_uid_(sp, messages = FALSE)[[1]],
                   "GBIF" = taxize::get_gbifid_(sp, messages = FALSE)[[1]],
                   "BOLD" = taxize::get_boldid_(sp, messages = FALSE)[[1]]
            )
          ))
        })

        auto_picked <- FALSE
        if(is.data.frame(res) && nrow(res) > 0) {
          names(res) <- tolower(names(res))
          sp_norm <- norm_name(sp)

          if(db == "NCBI") {
            if("scientificname" %in% names(res)) {
              res <- res[!is.na(res$scientificname), , drop = FALSE]
            } else { res <- res[0, , drop = FALSE] } # 没有有效列直接清零
          } else if(db == "BOLD") {
            n_col <- if("taxon" %in% names(res)) "taxon" else if("tax" %in% names(res)) "tax" else if("name" %in% names(res)) "name" else NULL
            if(!is.null(n_col)) {
              res <- res[!is.na(res[[n_col]]), , drop = FALSE]
            } else { res <- res[0, , drop = FALSE] }
          } else if(db == "GBIF") {
            if(!("canonicalname" %in% names(res) || "scientificname" %in% names(res))) {
              res <- res[0, , drop = FALSE]
            }
          }

          if(nrow(res) > 0) {

            if(db == "NCBI") {
              match_cond <- (norm_name(res$scientificname) == sp_norm | startsWith(norm_name(res$scientificname), paste0(sp_norm, " ")))
              match_cond[is.na(match_cond)] <- FALSE
              exact <- res[match_cond, , drop = FALSE]
              if(nrow(exact) > 0) { best_id <- exact$uid[1]; auto_picked <- TRUE }

            } else if(db == "GBIF") {
              match_cond <- rep(FALSE, nrow(res))
              if("canonicalname" %in% names(res)) match_cond <- match_cond | (!is.na(res$canonicalname) & norm_name(res$canonicalname) == sp_norm)
              if("scientificname" %in% names(res)) {
                sci_norm <- norm_name(res$scientificname)
                match_cond <- match_cond | (!is.na(sci_norm) & (sci_norm == sp_norm | startsWith(sci_norm, paste0(sp_norm, " "))))
              }
              match_cond[is.na(match_cond)] <- FALSE
              exact <- res[match_cond, , drop = FALSE]
              if(nrow(exact) > 0) {
                if("status" %in% names(exact)) { acc <- exact[toupper(exact$status) == "ACCEPTED", , drop=FALSE]; if(nrow(acc)>0) exact <- acc }
                best_id <- if("usagekey" %in% names(exact)) exact$usagekey[1] else exact$key[1]
                auto_picked <- TRUE
              }

            } else if(db == "BOLD") {
              cand_n <- res[[n_col]]
              match_cond <- (norm_name(cand_n) == sp_norm | startsWith(norm_name(cand_n), paste0(sp_norm, " ")))
              match_cond[is.na(match_cond)] <- FALSE
              exact <- res[match_cond, , drop = FALSE]
              if(nrow(exact) > 0) { best_id <- exact$taxid[1]; auto_picked <- TRUE }
            }

            if(auto_picked) {
              df_auto_recovered <- rbind(df_auto_recovered, data.frame(preprocessed.name = sp, taxid = as.character(best_id), stringsAsFactors = FALSE))
            } else {
              cands_list[[sp]] <- head(res, 3)
            }
          }
        }
        setTxtProgressBar(pb_man, i)
      }
      close(pb_man)

      if(length(cands_list) > 0) {
        if(!interactive) {
          cat(sprintf("  => 【提示】自动化模式已开启，跳过 %d 项多歧义物种的人工复核。\n", length(cands_list)))
        } else {
          cat(sprintf("\n【人工复核】 仍有 %d 个物种存在歧义，请手动确认：\n", length(cands_list)))
          c_name <- c(); c_id <- c()
          for(sp in names(cands_list)) {
            df <- cands_list[[sp]]; cat(sprintf("\n---  %s --- 【人工挑选】 待选:\n", sp))

            get_v <- function(r, cols) { for(c in cols) if(c %in% names(df) && !is.na(df[[c]][r])) return(as.character(df[[c]][r])); return("NA") }
            for(k in 1:nrow(df)) {
              if(db == "NCBI") cat(sprintf(" [%d] %-30s (级别: %-8s | 分区: %s)\n", k, get_v(k, "scientificname"), get_v(k, "rank"), get_v(k, "division")))
              if(db == "GBIF") cat(sprintf(" [%d] %-30s (级别: %-8s | 状态: %-8s | 目: %s)\n", k, get_v(k, c("canonicalname", "scientificname")), get_v(k, "rank"), get_v(k, "status"), get_v(k, "order")))
              if(db == "BOLD") cat(sprintf(" [%d] %-30s (级别: %-8s | 分区: %s)\n", k, get_v(k, c("taxon", "tax", "name")), get_v(k, "tax_rank"), get_v(k, "tax_division")))
            }
            cat(" [0] 均不匹配，跳过\n")
            ans_num <- -1
            while(!(ans_num %in% 0:nrow(df))) { ans_num <- suppressWarnings(as.integer(trimws(readline(prompt = "请输入序号: ")))) }
            if(ans_num > 0) {
              c_name <- c(c_name, sp)
              c_id <- c(c_id, if(db=="NCBI") get_v(ans_num, "uid") else if(db=="GBIF") get_v(ans_num, c("usagekey","key")) else get_v(ans_num, "taxid"))
            }
          }
          if(length(c_id) > 0) df_manual_pass <- data.frame(preprocessed.name = c_name, taxid = c_id, stringsAsFactors = FALSE)
        }
      }

      recovered_all <- bind_rows(df_auto_recovered, df_manual_pass)
      if(nrow(recovered_all) > 0) {
        out_id <- out_id %>% filter(!preprocessed.name %in% recovered_all$preprocessed.name) %>% bind_rows(recovered_all)
      }
    }

    # ---------------- 3.3 动态分类信息解析与总科补齐 ----------------
    df_pass <- out_id %>% filter(!is.na(taxid))
    res_parsed <- data.frame(preprocessed.name = character(), taxid = character(), stringsAsFactors = FALSE)

    if(nrow(df_pass) > 0) {
      cat(sprintf("  => 正在提取并解析分类信息...\n"))
      classf <- safe_classification(df_pass$taxid, tolower(db))

      if(group == "Animal") {
        parsed <- parse_taxonomy_Ani(classf, db_name = db)
        res_parsed <- df_pass %>% left_join(parsed, by = "taxid")

        if(db == "BOLD") {
          k_col <- paste0(db, ".kingdom")
          if(!k_col %in% names(res_parsed)) res_parsed[[k_col]] <- if(nrow(res_parsed) == 0) character(0) else NA_character_
          if(nrow(res_parsed) > 0) res_parsed[[k_col]][is.na(res_parsed[[k_col]])] <- "Animalia"
        }

        if(db != "NCBI") {
          fam_col <- paste0(db, ".family")
          if(!fam_col %in% names(res_parsed)) res_parsed[[fam_col]] <- if(nrow(res_parsed) == 0) character(0) else NA_character_
          if(nrow(res_parsed) > 0) {
            cat("  => 正在跨库 (NCBI) 匹配总科 (Superfamily)...\n")
            fam_to_super <- get_ncbi_superfamily_Ani(res_parsed[[fam_col]])
            res_parsed <- res_parsed %>% left_join(fam_to_super, by = setNames("family", fam_col))
          }
        }

      } else {
        parsed <- parse_taxonomy_Pla(classf, db_name = db)
        res_parsed <- df_pass %>% left_join(parsed, by = "taxid")

        if(db == "BOLD") {
          k_col <- paste0(db, ".kingdom")
          if(!k_col %in% names(res_parsed)) res_parsed[[k_col]] <- if(nrow(res_parsed) == 0) character(0) else NA_character_
          if(nrow(res_parsed) > 0) res_parsed[[k_col]][is.na(res_parsed[[k_col]])] <- "Plantae"
        }
      }
    }

    # ---------------- 3.4 字段对齐与漏斗分离 ----------------
    expected_cols <- paste0(db, ".", c("kingdom", "phylum", "class", "order", "family", "genus", "species"))
    if(group == "Animal") expected_cols <- c(expected_cols, "NCBI.superfamily")

    for(col in expected_cols) if(!col %in% names(res_parsed)) res_parsed[[col]] <- if(nrow(res_parsed) == 0) character(0) else NA_character_

    res_merged <- df_not_found %>%
      left_join(out_id, by = "preprocessed.name") %>%
      left_join(res_parsed, by = c("preprocessed.name", "taxid")) %>%
      mutate(source = case_when(
        is.na(taxid) ~ paste0(db, "_Not_Found"),
        preprocessed.name %in% df_auto_recovered$preprocessed.name ~ paste0(db, "_Auto_Recovered"),
        preprocessed.name %in% df_manual_pass$preprocessed.name ~ paste0(db, "_Manual_Pass"),
        TRUE ~ paste0(db, "_Strict")
      ))

    rename_logic <- list(
      input.name = "input.name", cleaned.name = "preprocessed.name", taxid = "taxid",
      kingdom = paste0(db, ".kingdom"), phylum = paste0(db, ".phylum"),
      class = paste0(db, ".class"), order = paste0(db, ".order")
    )
    if(group == "Animal") rename_logic$superfamily <- "NCBI.superfamily"
    rename_logic <- c(rename_logic, list(family = paste0(db, ".family"), genus = paste0(db, ".genus"), species = paste0(db, ".species"), source = "source"))

    found_data <- res_merged %>% filter(!is.na(taxid)) %>% select(any_of(unlist(rename_logic)))
    if(nrow(found_data) > 0) final_results_list[[db]] <- found_data

    df_not_found <- res_merged %>% filter(is.na(taxid)) %>% transmute(input.name, preprocessed.name = preprocessed.name)
  }

  # ===================== 4. 终极数据汇总与安全保存 =====================
  cat("\n========== 数据汇总与输出 ==========\n")
  res_missing_final <- data.frame()

  if(nrow(df_not_found) > 0) {
    missing_file <- file.path(output_dir, sprintf("Missing_Species_%s.xlsx", ifelse(group == "Animal", "Ani", "Pla")))

    # 保存 Missing_Species
    tryCatch({
      writexl::write_xlsx(df_not_found, missing_file)
      cat(sprintf("【提示】 彻底未查到的物种名单已保存至: %s\n", missing_file))
    }, error = function(e) {
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      fallback_path <- file.path(output_dir, sprintf("Missing_Species_%s_backup_%s.xlsx", ifelse(group == "Animal", "Ani", "Pla"), timestamp))
      writexl::write_xlsx(df_not_found, fallback_path)
      cat("\n====================== 【 警告 】 ======================\n")
      cat("检测到 Missing 目标 Excel 文件正在被打开，无法覆盖！\n")
      cat(sprintf("为了防止数据丢失，结果已自动另存为备用文件:\n  --> %s\n", fallback_path))
      cat("========================================================\n")
    })

    res_missing_final <- data.frame(
      input.name = df_not_found$input.name, cleaned.name = df_not_found$preprocessed.name, taxid = NA_character_,
      kingdom = NA_character_, phylum = NA_character_, class = NA_character_, order = NA_character_,
      family = NA_character_, genus = NA_character_, species = NA_character_, source = "Not Found",
      stringsAsFactors = FALSE
    )
    if(group == "Animal") res_missing_final$superfamily <- NA_character_
  }

  final_results_list[["Missing"]] <- res_missing_final
  combined_final <- bind_rows(final_results_list)

  # 终极保护 2：保存总结果大表
  final_save_path <- file.path(output_dir, sprintf("Unified_Combined_Results_%s.xlsx", ifelse(group == "Animal", "Ani", "Pla")))
  tryCatch({
    writexl::write_xlsx(combined_final, final_save_path)
    cat(sprintf("\n【 流水线完成 】 结果总表已保存至: %s\n", final_save_path))
  }, error = function(e) {
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    fallback_path <- file.path(output_dir, sprintf("Unified_Combined_Results_%s_backup_%s.xlsx", ifelse(group == "Animal", "Ani", "Pla"), timestamp))
    writexl::write_xlsx(combined_final, fallback_path)
    cat("\n====================== 【 警告 】 ======================\n")
    cat("检测到 Combined 总结果 Excel 文件正在被打开，无法覆盖！\n")
    cat(sprintf("为了防止数据丢失，总表已自动另存为备用文件:\n  --> %s\n", fallback_path))
    cat("========================================================\n")
  })

  return(invisible(combined_final))
}
