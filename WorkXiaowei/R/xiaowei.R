#' The function to connect to datdabases
#'
#' @param sql
#' @param database
#' @param queue
#'
#' @return results of sql
#' @export
#'
#' @examples
getMifiJdbcQuery <- function(sql=NULL, database='mifi', queue='loanba'){
  library(RJDBC)
  .jinit(parameters=c("-Dhadoop.property.hadoop.security.authentication=kerberos",
                      "-Djava.security.krb5.conf=/etc/krb5-hadoop.conf",
                      "-Dhadoop.property.hadoop.client.keytab.file=/home/work/rstudio-system/keytab/h_mifi.keytab",
                      "-Dhadoop.property.hadoop.client.kerberos.principal=h_mifi@XIAOMI.HADOOP"))


  # system("java -version")
  for(l in list.files("/home/work/rstudio-system/lib/jdbc-example-1.0-SNAPSHOT/lib/", "jar$")){
    .jaddClassPath(paste0("/home/work/rstudio-system/lib/jdbc-example-1.0-SNAPSHOT/lib/",l))
  }

  drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver", classPath = "/home/work/rstudio-system/lib/jdbc-example-1.0-SNAPSHOT/lib/hive-jdbc-0.13.1-cdh5.3.0.jar")

  ugi <- J("org.apache.hadoop.security.UserGroupInformation")$getLoginUser()
  ### database setting
  con_setting = "jdbc:hive2://zjyprc-hadoop.spark-sql.hadoop.srv:10000/%s;principal=sql_prc/hadoop@XIAOMI.HADOOP?spark.yarn.queue=root.service.miui_group.mifi.%s"
  con_setting = sprintf(con_setting,database,queue)
  con <- dbConnect(drv,con_setting)

  ###
  if(is.null(sql)) stop('no query sql...\n')
  ### add limit 100
  if(!grepl('limit',sql)) sql=paste0(sql,' limit 100')

  ####
  t1 = Sys.time()
  cat(sprintf('start time: %s\n',t1))
  cat('----------------------------------------\n')
  cat(sql)
  results =  dbGetQuery(con, sql)
  ##
  t2 = Sys.time()
  cat('----------------------------------------\n')
  cat(sprintf('end time: %s,time spend: %s\n',t2,t2-t1))
  # close db connection
  dbDisconnect(con)
  ####
  return(results)
}


#' xiaowei pass and use
#'
#' @param qudao
#'
#' @return dataframe
#' @export
#'
#' @examples
#'
func_credit_use <- function(qudao) {


  if (qudao == "小微大盘") {
    credit_data <- getMifiJdbcQuery(sprintf("select first_credit_time,
                                              count(distinct xiaomi_id) as n,
                                                sum(basicinfo_cash_amount/100) as sum_cash_amount
                                                from mifidw_sme_credit where `date` = %s
                                                and basicinfo_credit_status in (1, 3)
                                                and first_credit_time >= %s
                                                group by 1
                                                order by 1 desc limit 10000",
                                            Runtime, AcptTime_Begin))
    credit_data0 <- credit_data %>%
      mutate(name = "小微大盘") %>%
      arrange(-first_credit_time)


    use_data <- getMifiJdbcQuery(sprintf("select a.first_credit_time,
                                           count(distinct b.xiaomi_id) as use_num,
                                             sum(if(b.prin > 0, b.prin, 0)) as use_prin,

                                             count(distinct(case when b.balance > 0 then b.xiaomi_id else null end)) as bal_num,
                                             sum(if(b.balance > 0, a.cash_amount, 0)) as bal_cash_amount,
                                             sum(if(b.balance > 0, b.balance, 0)) as bal_bal

                                             from

                                             (select xiaomi_id, basicinfo_cash_amount/100 as cash_amount, first_credit_time
                                             from mifidw_sme_credit
                                             where `date` = %s and first_credit_time between %s and %s
                                             and basicinfo_credit_status in (1, 3)) a

                                             inner join

                                             (select `date`, xiaomi_id, sum(prin/100) as prin, sum(balance/100) as balance
                                             from mifidw_sme_contract_fact where `date` between %s and %s and effective_date = `date`
                                             group by `date`, xiaomi_id) b
                                             on a.xiaomi_id = b.xiaomi_id

                                             where a.first_credit_time = b.`date`
                                             group by a.first_credit_time
                                             order by a.first_credit_time DESC
                                             limit 100000",
                                         Runtime, AcptTime_Begin, Runtime,
                                         AcptTime_Begin, Runtime))
    use_data0 <- use_data %>%
      mutate(name = "小微大盘") %>%
      arrange(-first_credit_time)

  }

  else {
    credit_data <- getMifiJdbcQuery(sprintf("select first_credit_time,
                                              count(distinct xiaomi_id) as n,
                                                sum(basicinfo_cash_amount/100) as sum_cash_amount
                                                from mifidw_sme_credit where `date` = %s
                                                and basicinfo_credit_status in (1, 3)
                                                and %s is not null
                                                and first_credit_time >= %s
                                                group by 1
                                                order by 1 desc limit 10000",
                                            Runtime, qudao, AcptTime_Begin))
    credit_data0 <- credit_data %>%
      mutate(name = qudao) %>%
      arrange(-first_credit_time)


    use_data <- getMifiJdbcQuery(sprintf("select a.first_credit_time,
                                           count(distinct b.xiaomi_id) as use_num,
                                             sum(if(b.prin > 0, b.prin, 0)) as use_prin,

                                             count(distinct(case when b.balance > 0 then b.xiaomi_id else null end)) as bal_num,
                                             sum(if(b.balance > 0, a.cash_amount, 0)) as bal_cash_amount,
                                             sum(if(b.balance > 0, b.balance, 0)) as bal_bal

                                             from

                                             (select xiaomi_id, basicinfo_cash_amount/100 as cash_amount, first_credit_time
                                             from mifidw_sme_credit
                                             where `date` = %s and first_credit_time between %s and %s
                                             and basicinfo_credit_status in (1, 3)
                                             and %s is not null) a

                                             inner join

                                             (select `date`, xiaomi_id, sum(prin/100) as prin, sum(balance/100) as balance
                                             from mifidw_sme_contract_fact where `date` between %s and %s and effective_date = `date`
                                             and %s is not null
                                             group by `date`, xiaomi_id) b
                                             on a.xiaomi_id = b.xiaomi_id

                                             where a.first_credit_time = b.`date`
                                             group by a.first_credit_time
                                             order by a.first_credit_time DESC
                                             limit 100000",
                                         Runtime, AcptTime_Begin, Runtime, qudao,
                                         AcptTime_Begin, Runtime, qudao))
    use_data0 <- use_data %>%
      mutate(name = qudao) %>%
      arrange(-first_credit_time)
  }

  credit_use_data <- credit_data0 %>%
    left_join(use_data0)

  credit_use_data0 <- credit_use_data %>%
    mutate(week_num = week(ymd(first_credit_time))) %>%
    left_join(group_by(., week_num) %>%
                summarise(week_dur = paste0(str_sub(min(first_credit_time), 5, 8), "~", str_sub(max(first_credit_time), 5, 8)))) %>%
    group_by(week_num, week_dur, name) %>%
    summarise_at(vars(n:bal_bal), sum, na.rm = TRUE) %>%
    mutate(avg_cash_amount = sum_cash_amount/n,
           avg_use_prin = use_prin/use_num,
           avg_bal = bal_bal/bal_num,
           use_rate_num = use_num/n,
           use_rate_amt = paste0(round(bal_bal/bal_cash_amount, 4) * 100, "%")) %>%
    select(week_num, week_dur, name, n, avg_cash_amount, use_num, bal_num, avg_use_prin, use_prin,
           avg_bal, bal_bal, use_rate_num, use_rate_amt) %>%
    arrange(-week_num, week_dur, name) %>%
    rename("第几周" = week_num,
           "时段" = week_dur,
           "授信人数" = n,
           "平均授信额度" = avg_cash_amount,
           "支用人数（当天）" = use_num,
           "余额人数" = bal_num,
           "平均支用金额" = avg_use_prin,
           "总支用金额" = use_prin,
           "平均余额" = avg_bal,
           "总余额" = bal_bal,
           "人数支用率" = use_rate_num,
           "额度使用率" = use_rate_amt) %>%
    as.data.frame(.)

  return (credit_use_data0)

}

