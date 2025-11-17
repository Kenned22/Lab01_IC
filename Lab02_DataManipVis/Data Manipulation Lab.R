#Load relevant libraries - update as needed if there are functions you need that are not in existing libraries
library(tidyverse)
library(arrow)


#Load and view data - every person make sure you have the data uploaded yourself and added to git ignore
bids <- read_parquet("C:/Users/LillyKennedy/OneDrive - Portland Psychotherapy/Documents/Lab01_IC/data/bids_data_vDTR.parquet")
glimpse(bids)

