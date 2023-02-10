rm(list = ls())

# DEPENDENCIES ------------------------------------------------------------


library(tidyverse)
library(glamr)
library(googlesheets4)
library(googledrive)
library(fs)
library(lubridate)
library(janitor)
library(readxl)
library(openxlsx)
library(glue)
library(gt)




# TPT - BATCH PROCESS HISTORIC FILES -------------------------------------

# paths & values
path_monthly_output_repo <- "Dataout/monthly_processed/" # path to repo where original files are stored
path_new_directory <- "Dataout/monthly_processed_grouped/" # path to repo where corrected files will be saved


# batch load files to be processed
batch_list <- path_monthly_output_repo %>% 
  dir_ls() %>% 
  map(
    .f = function(path){
      read_tsv(
        path
      )
    }
  )


# set names, bind rows, and replace data_uids
batch_data_tbl <- batch_list %>% 
  set_names(dir_ls(path_monthly_output_repo)) %>% 
  bind_rows(.id = "file_path") %>% 
  group_by(period, snu, psnu, sitename, disa_uid, site_nid, age, group, sex, motive, tat_step) %>% 
  summarise(VL = sum(VL),
            VLS = sum(VLS),
            TAT = sum(TAT)) %>%
  ungroup() %>% 
  mutate(TAT = case_when(is.na(tat_step) ~ NA_integer_,
                         TRUE ~ TAT),
         VL = case_when(!is.na(tat_step) ~ NA_integer_,
                        TRUE ~ VL),
         VLS = case_when(!is.na(tat_step) ~ NA_integer_,
                        TRUE ~ VLS))



# create new repo
dir_create(path_new_directory)


# batch save new files
batch_data_tbl %>% 
  mutate(file_path = period) %>% 
  group_by(file_path) %>% 
  group_split(.keep = FALSE) %>% 
  map(
    .f = function(data){
      filename <- unique(data$period) %>% # create a period-based value that will be used for naming the file
        format(., "%Y_%m") # reformat above value to only include year_month with underscore separation
      write_tsv(data, file = glue::glue("{path_new_directory}DISA_VL_{filename}.txt"))
    }
  )
