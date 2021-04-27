library(tidyverse)
library(dplyr)
library(data.table)
library(vroom)
library(lubridate)

list.files(path = "data/raw", pattern = ".csv")

# "apbn-2019-belanja-kl-per-fungsi-gfs.csv"
# "apbn-2019-belanja-kl-per-fungsi-gfs__2019.csv"
# "apbn-2019-belanja-kl.csv"
# "apbn-2019-belanja-kl__2019.csv"               
# "apbn-2019 belanja-fungsi-2019.csv"
# "apbn-2019 belanja-fungsi-2019__2019.csv"

# "data-djpk-apbd-2018-created_at-2021-04-27.csv"
# "data-djpk-apbd-2019-created_at-2021-04-27.csv"
# "data-djpk-apbd-2020-created_at-2021-04-27.csv"
# "data-djpk-apbd_2010-2021-04-27.csv"           
# "djpk_apbd_2010-2020.csv" 



df2 <- vroom("data/raw/data-djpk-apbd-2019-created_at-2021-04-27.csv")

df2 <- df2 %>% select(-special_row) %>%
  gather(key = kode, value = nilai, contains("_")) %>%
  mutate(jenis_data = case_when(str_detect(kode, "_a") ~ "anggaran",
                                str_detect(kode, "_r") ~ "realisasi",
                                str_detect(kode, "_p") ~ "persentase"),
         kode = str_remove_all(kode, "[_arp]")) %>%
  pivot_wider(names_from = jenis_data, values_from = nilai) %>% 
  mutate(persentase = str_remove(persentase, " %"),
         persentase = gsub(",", ".", persentase))



# data from DJPK
df1 <- read.csv("https://raw.githubusercontent.com/fitrajateng/automasi-portal-djpk/main/data/data-apbd-2010-2020-11-15--05-15.csv")

df1 <- df1 %>% select(-special_row) %>%
  gather(key = kode, value = nilai, starts_with("X")) %>%
  mutate(jenis_data = case_when(str_detect(kode, "_a") ~ "anggaran", 
                                str_detect(kode, "_r") ~ "realisasi", 
                                str_detect(kode, "_p") ~ "persentase"),
         kode = str_remove_all(kode, "[X_arp]")) %>%
  pivot_wider(names_from = jenis_data, values_from = nilai) %>%
  mutate(persentase = str_remove(persentase, " %"),
         persentase = gsub(",", ".", persentase))

df_join <- bind_rows(df1, df2)

# recode kode struktur anggaran
apbd_all <- df_join %>% mutate(akun = recode(kode,
                                                  "400"="Pendapatan",
                                                  "410"="PAD",
                                                  "411"="Pajak Daerah",
                                                  "412"="Retribusi Daerah",
                                                  "413"="Hasil pengelolaan kekayaan daerah yang dipisahkan",
                                                  "414"="Lain-lain PAD yang sah",
                                                  "420"="Dana Perimbangan",
                                                  "421"="DBH pajak/bukan pajak",
                                                  "422"="DAU",
                                                  "423"="DAK",
                                                  "430"="Lain-lain pendapatan daerah yang sah",
                                                  "431"="Hibah",
                                                  "432"="Dana darurat",
                                                  "433"="DBH pajak dari Propinsi dan Pemda lainnya",
                                                  "434"="Dana penyesuaian dan otonomi khusus",
                                                  "435"="Bantuan keuangan dari Propinsi atau Pemda lainnya",
                                                  "436"="Lain-lain",
                                                  "439"="Pendapatan Lainnya",
                                                  "500"="Belanja",
                                                  "510"="Belanja Tidak Langsung",
                                                  "511"="Belanja Pegawai",
                                                  "512"="Belanja Bunga",
                                                  "513"="Belanja Subsidi",
                                                  "514"="Belanja Hibah",
                                                  "515"="Belanja Bantuan sosial",
                                                  "516"="Belanja Bagi hasil kpd Prop/Kab/Kota dan Pemdes",
                                                  "517"="Belanja Bantuan keuangan kpd Prop/Kab/Kota dan Pemdes",
                                                  "518"="Belanja tidak terduga",
                                                  "519"="Belanja Lainnya",
                                                  "520"="Belanja Langsung",
                                                  "521"="Belanja Pegawai",
                                                  "522"="Belanja Barang dan Jasa",
                                                  "523"="Belanja Modal",
                                                  "600"="Pembiayaan",
                                                  "610"="Penerimaan",
                                                  "611"="SiLPA TA sebelumnya",
                                                  "612"="Pencairan dana cadangan",
                                                  "613"="Hasil Penjualan Kekayaan Daerah yang Dipisahkan",
                                                  "614"="Penerimaan Pinjaman Daerah dan Obligasi Daerah",
                                                  "615"="Penerimaan Kembali Pemberian Pinjaman",
                                                  "616"="Penerimaan piutang daerah",
                                                  "617"="Penerimaan Kembali Investasi Dana Bergulir",
                                                  "619"="Penerimaan Pembiayaan Lainnya",
                                                  "620"="Pengeluaran",
                                                  "621"="Pembentukan Dana Cadangan",
                                                  "622"="Penyertaan Modal (Investasi) Daerah",
                                                  "623"="Pembayaran Pokok Utang",
                                                  "624"="Pemberian Pinjaman Daerah",
                                                  "625"="Pembayaran Kegiatan Lanjutan",
                                                  "626"="Pengeluaran Perhitungan Pihak Ketiga",
                                                  "629"="Lainnya"),
                                    anggaran = as.double(anggaran),
                                    realisasi = as.double(realisasi),
                                    pct_lra = realisasi/anggaran * 100)

# parsing date from disclaimer
# unique(apbd_all$disclaimer)

apbd_prep <- apbd_all %>% mutate(tanggal = disclaimer,
                                          tanggal = gsub("Anggaran) ", "Anggaran);", tanggal),
                                          tanggal = gsub("2018) ", "2018);", tanggal),
                                          tanggal = gsub("s.d. ", ";", tanggal),
                                          tanggal = gsub("APBD 2019 ", ";", tanggal)
                                          ) %>%
  separate(tanggal, into = c("date.apbd", "date.lra"), sep = ";")

# unique(apbd_prep$tanggal)
unique(apbd_prep$date.apbd)
unique(apbd_prep$date.lra)


# change date.apbd and date.lra 

apbd_prep <- apbd_prep %>% 
  mutate(date.apbd = gsub("20 Juli 2010", "2010-07-20", date.apbd),
         date.apbd = gsub("5 Juli 2011", "2011-07-05", date.apbd),
         date.apbd = gsub("29 Mei 2012", "2012-05-29", date.apbd),
         date.apbd = gsub("15 Juli 2013", "2013-07-15", date.apbd),
         date.apbd = gsub("Oktober 2014", "2014-10-01", date.apbd),
         date.apbd = gsub("April 2015", "2015-04-01", date.apbd),
         date.apbd = gsub("Februari 2017", "2017-02-01", date.apbd),
         date.apbd = gsub("26 Mei 2017", "2017-05-26", date.apbd),
         date.apbd = gsub("5 Juli 2018", "2018-07-05", date.apbd),
         date.apbd = gsub("[A-z]", "", date.apbd),
         date.apbd = gsub("[ \\()]", "", date.apbd),
         
         # cleaning date.lra
         date.lra = gsub("Nov ", "November ", date.lra),
         date.lra = gsub("Okt ", "Oktober ", date.lra),
         date.lra = gsub("4 November 2016", "2016-11-04", date.lra),
         date.lra = gsub("21 Oktober 2016", "2016-10-21", date.lra),
         date.lra = gsub("4 Juli 2017", "2017-07-04", date.lra),
         date.lra = gsub("11 Oktober 2018", "2018-10-11", date.lra),
         date.lra = gsub("12 November 2018", "2018-11-12", date.lra),
         date.lra = gsub("28 Januari 2020", "2020-01-28", date.lra),
         date.lra = gsub("September 2020", "2020-09-01", date.lra),
         date.lra = gsub("21 Oktober 2020", "2020-10-21", date.lra),
         date.lra = gsub("[ \\()]", "", date.lra),
         date.lra = gsub("[A-z]", "", date.lra),
         
         # convert with lubridate
         date.apbd = ymd(date.apbd),
         date.lra = ymd(date.lra)
         ) %>%
  select(-disclaimer)

skimr::skim(apbd_prep)
glimpse(apbd_prep)

apbd_clean <- apbd_prep

write.csv(apbd_clean, "data/clean/djpk_apbd-2010-2020.csv")

