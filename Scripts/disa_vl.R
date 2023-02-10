
rm(list = ls())

# DEPENDENCIES ------------------------------------------------------------


library(tidyverse)
library(mozR)
library(fs)
library(googlesheets4)
library(googledrive)
library(glamr)
library(glitr)
library(grabr)
library(ggthemes)
library(janitor)
library(glue)
library(readxl)
library(openxlsx)
library(Wavelength)
load_secrets()


# GLOBAL VARIABLES --------------------------------------------------------


# submission month and file path - update each month
month <- "2023-01-20"
filename <- "Data/Relatorio Mensal de Carga Viral Janeiro 2023.xlsx"

# no monthly updates needed
dt <- base::format(as.Date(month), 
                   "%Y_%m")

file <- glue::glue("DISA_VL_{dt}")

path_monthly_output_local <- "Dataout/monthly_processed/"
file_monthly_output_local <- path(path_monthly_output_local, file, ext = "txt")
path_monthly_output_gdrive <- as_id("https://drive.google.com/drive/folders/12XN6RKaHNlmPoy3om0cbNd1Rn4SqHSva")
file_historic_output_local <- "Dataout/em_disa.txt"
path_historic_output_gdrive <- as_id("https://drive.google.com/drive/folders/1xBcPZNAeYGahYj_cXN5aG2-_WSDLi6rQ")


# PULL METADATA ------------------------------------------------------


disa_datim_map <- pull_sitemap(sheet = "map_disa") %>% 
  select(!c(note))


ajuda_site_map <- pull_sitemap(sheet = "list_ajuda") %>% 
  select(datim_uid,
         partner = partner_pepfar_clinical,
         starts_with("his_"))


cntry <- "Mozambique"
uid <- get_ouuid(cntry)
datim_orgsuids <- pull_hierarchy(uid, username = datim_user(), password = datim_pwd()) %>% 
  filter(!is.na(facility) & !is.na(psnu)) %>% 
  select(datim_uid = orgunituid,
         snu = snu1,
         psnu,
         sitename = facility) %>% 
  arrange(snu, psnu, sitename)


# INGEST DISA SUBMISSION --------------------------------------------------


df <- process_disa_vl(filename)


# tabulate sites that have viral load results reported
df %>% 
  filter(!is.na(disa_uid)) %>% 
  group_by(snu) %>% 
  distinct(sitename) %>% 
  summarise(n()) %>% 
  arrange(`n()`)

# tabulate sites missing disa unique identifiers
df %>% 
  filter(is.na(disa_uid)) %>% 
  group_by(snu) %>% 
  distinct(sitename) %>% 
  summarise(n()) %>% 
  arrange(`n()`)


# PRINT MONTHLY OUTPUT ----------------------------------------------------


readr::write_tsv(
  df,
  {file_monthly_output_local},
  na ="")

drive_put(file_monthly_output_local,
          path = path_monthly_output_gdrive,
          name = glue({file}, '.txt'))


# SURVEY MONTHLY DATASETS AND COMPILE ----------------------------


historic_files <- dir({path_monthly_output_local}, pattern = "*.txt")  # PATH FOR PURR TO FIND MONTHLY FILES TO COMPILE

df_historic <- historic_files %>%
  map(~ read_tsv(file.path(path_monthly_output_local, .))) %>%
  reduce(rbind)


df_meta <- df_historic %>% 
  select(!site_nid) %>% 
  left_join(disa_datim_map, by = c("disa_uid" = "disa_uid")) %>% 
  mutate(ajuda = replace_na(ajuda, 0)) %>% 
  relocate(c(ajuda, sisma_uid, datim_uid), .before = disa_uid)


df_final <- disa_meta %>% 
  
  drop_na(datim_uid) %>%
  select(!c(snu, psnu, sitename)) %>% 
  left_join(datim_orgsuids, by = c("datim_uid" = "datim_uid")) %>%
  left_join(ajuda_site_map, by = c("datim_uid" = "datim_uid")) %>% 
  mutate(
    partner = replace_na(partner, "MISAU"),
    
    support_type = case_when(
      partner == "MISAU" ~ "Sustainability",
      TRUE ~ as.character("AJUDA")),
    
    agency = case_when(
      partner == "MISAU" ~ "MISAU",
      partner == "JHPIEGO-DoD" ~ "DOD",
      partner == "ECHO" ~ "USAID",
      TRUE ~ as.character("HHS/CDC")),
    
    snu = case_when(
      partner == "JHPIEGO-DoD" ~ "_Military",
      TRUE ~ snu),
    
    psnu = case_when(
      partner == "JHPIEGO-DoD" ~ "_Military",
      TRUE ~ psnu),
    
    sitename = case_when(
      partner == "JHPIEGO-DoD" ~ "_Military",
      TRUE ~ sitename)) %>% 
  
  select(period,
         sisma_uid,
         datim_uid,
         site_nid,
         starts_with("his_"),
         snu,
         psnu,
         sitename,
         support_type,
         partner,
         agency,
         age,
         group,
         sex,
         motive,
         tat_step,
         VL,
         VLS,
         TAT) %>% 
  glimpse()


# CHECK RESULTS LOST WHEN FILTERING ON DATIM_UID --------------------------


df_missing <- df_meta %>% 
  filter(is.na(datim_uid),
         group == "Age") %>% 
  group_by(period, snu, psnu, sitename, disa_uid) %>% 
  summarize(
    across(c(VL, VLS, TAT), .fns = sum), .groups = "drop")

sum(df_final$VL, na.rm = T)
sum(df_missing$VL, na.rm = T)



# PLOT HISTORIC DATA ----------------------------------------------


df_vl_plot <- df_final %>% 
  filter(!is.na(group)) %>% 
  group_by(period, group, partner, snu) %>% 
  summarize(VL = sum(VL, na.rm = T)) %>% 
  ungroup()

df_vl_plot %>% 
  ggplot(aes(x = period, y = VL, fill = snu)) + 
  geom_col() + 
  labs(title = "TX_CURR Trend by Partner",
       subtitle = "Historical Trend of Patients on ART in Mozambique by PEPFAR Partner",
       color = "Partner") + 
  theme_solarized() + 
  theme(axis.title = element_text()) + 
  facet_wrap(~group)


# PRINT OUTPUTS -----------------------------------------------------------


write.xlsx(disa_missing,
           {"Dataout/missing_sites_mfl.xlsx"},
           overwrite = TRUE)


readr::write_tsv(
  df_final,
  {file_historic_output_local},
  na = "")


drive_put(file_historic_output_local,
          path = path_historic_output_gdrive)


