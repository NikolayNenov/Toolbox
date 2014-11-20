library(plyr)
library(RODBC)
library(sqldf)


conn<-odbcDriverConnect("dsn=DBRAI;Server=localhost;uid=nikolay.nenov; pwd=X7z0WvwRgMSBOZtCZfBx;")
# 
# 
# 
users <- sqlQuery(conn,
                  ("select
                   distinct(u.id) as usuarioid
                   , u.pais as reg_country
                   , date(u.fecha_alta) as fecha_alta
                   , case when u.primer_pago='0000-00-00' then null
                   else u.primer_pago end as primer_pago 
                   , case when u.primer_pago='0000-00-00' then null
                   else  datediff(u.primer_pago, date(u.fecha_alta)) end as pp_rec
                   , u.partnerid
                   , u.plataforma
                   , u.juego_usu
                   , c.utm_medium
                   , c.utm_campaign
                   , c.utm_source
                   from usuarios u
                   inner join usu_campanya c ON u.id = c.usuarioid  
                   WHERE u.primer_pago between '2012-01-01' and '2012-12-30'
                   ;"))


#user<-read.csv(file="C://Users//nikolay.nenov//Documents//Projects//2013-11 LTV Modeling//PoC_users.csv", sep=";",header=TRUE);


users$primer_pago<-as.Date(as.character(users$primer_pago), "%Y-%m-%d")


#View(users)




transactional <- sqlQuery(conn, paste
                          ("select
                           usuarioid
                           , date(fecha) as fecha
                           , SUM( fCurrencyToEur(moneda, base_imponible, fecha) ) AS bi
                           , SUM( fCurrencyToEur(moneda, importe_neto, fecha))  AS net_rev
                           , count(p.id) as num_payments
                           from pagosfacturas_view
                           WHERE p.usuarioid in ('",
                           paste(unique(subset(users, !is.na(primer_pago))$usuarioid),collapse="','"),
                           "')
                           and not (medio = 'mobiadvanced' and base_imponible = 6 and moneda = 'Eur')
                           group by usuarioid, date(fecha)
                           ;"))


transactional$fecha<-as.Date(as.character(transactional$fecha), "%Y-%m-%d")

#View(transactional)


db<-sqldf("select
          u.usuarioid
          , u.reg_country
          , u.fecha_alta
          , (t.fecha - u.fecha_alta) as days_alta
          , primer_pago
          , (t.fecha - u.primer_pago) as days_pago
          , num_payments
          , bi
          , net_rev
          from 'users' as u
          left join 'transactional' as t on u.usuarioid=t.usuarioid ")


db_clean<-subset(db,!is.na(primer_pago) & days_pago<=365)
db_clean$days_pago<-as.numeric(db_clean$days_pago)


db_clean$range_pp <- cut(db_clean$days_pago, breaks=c(-1,30,61,91,122,152,183,213,244,274,304,335,365), labels=c(1,2,3,4,5,6,7,8,9,10,11,12))

#View(db_clean)


#Segmenting: Take all usuarioid where any given month the user paid more than €100

clust=subset(sqldf("select usuarioid, sum(bi) as bi, range_pp from 'db_clean' group by usuarioid, range_pp order by bi desc"),
            bi>=100)
#View(clust)


distr<-sqldf("select usuarioid, min(range_pp) as range_pp from 'clust' group by usuarioid order by bi desc")

library(rattle)
rattle()

#Add segment variable
db_clean$segm<-ifelse(db_clean$usuarioid %in% unique(clust$usuarioid), 'Whale','Other')




plot<-subset(sqldf("select count(distinct(usuarioid)) as users, sum(bi) as bi
                   , sum(num_payments) as num_payments, segm, days_pago
                   from 'db_clean'
                   group by segm, days_pago"),
             days_pago>=0)


#View(plot)


final<-within(db_clean, {cumm_bi<-ave(bi, usuarioid, FUN=cumsum)})


#View(final)



#function to estimate sensitivity (true positive) of whales captured

sensitivity_users<-function(day, cummbi){
  results<-length(unique(subset(final, segm=='Whale' & days_pago<=day
                                & cumm_bi>= cummbi)$usuarioid))/length(unique(subset(final, segm=='Whale')$usuarioid))
  return(results)
}



#Run sensitivity
sensitivity=c(
  sensitivity_users(0, 5.762)
  ,sensitivity_users(1, 8.979)
  ,sensitivity_users(2, 11.704)
  ,sensitivity_users(3, 13.410)
  ,sensitivity_users(4, 18.878)
  ,sensitivity_users(5, 20.995)
  ,sensitivity_users(6, 22.963)
  ,sensitivity_users(7, 25.691)
  ,sensitivity_users(8, 27.738)
  ,sensitivity_users(9, 30.158)
  ,sensitivity_users(10, 32.628)
  ,sensitivity_users(11, 34.181)
  ,sensitivity_users(12, 36.327)
  ,sensitivity_users(13, 38.053)
  ,sensitivity_users(14, 39.811)
  ,sensitivity_users(15, 41.290)
  ,sensitivity_users(16, 43.433)
  ,sensitivity_users(17, 45.087)
  ,sensitivity_users(18, 46.542)
  ,sensitivity_users(19, 47.724)
  ,sensitivity_users(20, 49.067)
  ,sensitivity_users(21, 50.239)
  ,sensitivity_users(22, 51.986)   
  ,sensitivity_users(23, 53.863)   
  ,sensitivity_users(24, 56.016)   
  ,sensitivity_users(25, 58.128)   
  ,sensitivity_users(26, 59.763)   
  ,sensitivity_users(27, 61.676)   
  ,sensitivity_users(28, 63.563)
  ,sensitivity_users(29, 64.951)
  ,sensitivity_users(30, 67.295)
)



#function to estimate specificity (false positive) of whales captured


specificity_users<-function(day, cummbi){
  results<-
    length(unique(subset(final, segm=='Other' & days_pago<=day & cumm_bi>= cummbi)$usuarioid))/ #true negatives
    length(unique(subset(final, segm=='Other')$usuarioid)) #total others
  return(results)
}

specificity=c(
  specificity_users(0, 5.762)
  ,specificity_users(1, 8.979)
  ,specificity_users(2, 11.704)
  ,specificity_users(3, 13.410)
  ,specificity_users(4, 18.878)
  ,specificity_users(5, 20.995)
  ,specificity_users(6, 22.963)
  ,specificity_users(7, 25.691)
  ,specificity_users(8, 27.738)
  ,specificity_users(9, 30.158)
  ,specificity_users(10, 32.628)
  ,specificity_users(11, 34.181)
  ,specificity_users(12, 36.327)
  ,specificity_users(13, 38.053)
  ,specificity_users(14, 39.811)
  ,specificity_users(15, 41.290)
  ,specificity_users(16, 43.433)
  ,specificity_users(17, 45.087)
  ,specificity_users(18, 46.542)
  ,specificity_users(19, 47.724)
  ,specificity_users(20, 49.067)
  ,specificity_users(21, 50.239)
  ,specificity_users(22, 51.986)   
  ,specificity_users(23, 53.863)   
  ,specificity_users(24, 56.016)   
  ,specificity_users(25, 58.128)   
  ,specificity_users(26, 59.763)   
  ,specificity_users(27, 61.676)   
  ,specificity_users(28, 63.563)
  ,specificity_users(29, 64.951)
  ,specificity_users(30, 67.295)
)


day=c(0:30)



sty_spy<-data.frame(day=day, true_positive=sensitivity, false_positive=specificity)


pairs(~day+true_positive+false_positive, data=sty_spy, main="Response Captured from using BI per day variable")


