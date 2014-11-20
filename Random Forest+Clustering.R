library(plyr)
library(RODBC)
library(sqldf)
library(reshape2)
library(gtools)
library(pingr)
library(ppls)
Sys.setenv(LANGUAGE="en")

library(randomForest)


conn<-odbcDriverConnect("dsn=DBRAI;Server=localhost;uid=nikolay.nenov; pwd=X7z0WvwRgMSBOZtCZfBx;")




#xxxxxxxxxxxxxxxxxx#
#### Pull users ####
#xxxxxxxxxxxxxxxxxx#



users_depl <- sqlQuery(conn,
                       ("select
                   distinct(u.id) as usuarioid
                   , usuario as name
                   , u.pais as reg_country
                   , date(u.fecha_alta) as fecha_alta
                   , u.primer_pago
                   , case when u.primer_pago='0000-00-00' then null
                   else  datediff(u.primer_pago, date(u.fecha_alta)) end as pp_rec
                   , p.company
                   , u.plataforma
                   , case when u.juego_usu in (18, 41, 44, 48) then 'Bingo'
                          when u.juego_usu in (27, 47, 32, 12, 20, 30, 34, 46) then 'Other_Casino'
                          when u.juego_usu in (29, 8, 36, 28, 9, 42, 19, 17, 14, 11, 45, 26, 38, 33, 39, 5, 31, 37, 35) then 'Cartas'
                          when u.juego_usu in (13, 10, 1, 25, 7, 3) then 'Mesa'
                          when u.juego_usu in (21, 15, 2, 16, 22, 6, 4) then 'Habilidad'
                          else 'Desconocido'
                    end as acq_juego_category
                   , j.descripcion as juego_usu
                   , c.utm_medium
                   , c.utm_campaign
                   , c.utm_source
                   from usuarios u
                   left join usu_campanya c ON u.id = c.usuarioid
                   left join partner as p on u.partnerid=p.id
                   left join juego j on u.juego_usu=j.game_id
                   WHERE u.primer_pago > '2014-10-12' 
                        and u.primer_pago < DATE_SUB(curdate(), INTERVAL 15 DAY)
                        ;"))

#DATE_SUB(curdate(), INTERVAL 15 DAY)
#View(users_depl)
# max(users_depl$primer_pago)

# select max(primer_pago) from usuarios where id in (select usuarioid from estadisticas.usu_ltv);


users_depl$primer_pago<-as.Date(as.character(users_depl$primer_pago), "%Y-%m-%d")














#xxxxxxxxxxxxxxxxxxxxxxxxxxxx#
#xxxxxxxxxxxxxxxxxxxxxxxxxxxx#
####### Transactional ########
#xxxxxxxxxxxxxxxxxxxxxxxxxxxx#
#xxxxxxxxxxxxxxxxxxxxxxxxxxxx#



#xxxxxxxxxsxxxxxxxxxxxxxx#
#### * 15 days BI/Net ####
#xxxxxxxxxxsxxxxxxxxxxxxx#


bi_net_depl <- sqlQuery(conn, paste
                          ("select
                           p.usuarioid
                           , round(sum(fCurrencyToEur(moneda, base_imponible, p.fecha)),3) AS bi
                           , round(sum(fCurrencyToEur(moneda, importe_neto, p.fecha)),3)  AS nr
                           , count(p.id) as num_payments
                           , round(sum(fCurrencyToEur(moneda, base_imponible, p.fecha)),3)/count(p.id) as ATV_bi
                           , round(sum(fCurrencyToEur(moneda, importe_neto, p.fecha)),3) / count(p.id) as ATV_nr
                           from pagosfacturas_view  p
                           inner join usuarios u on u.id=p.usuarioid
                           
                           WHERE p.usuarioid in ('",
                           paste(unique(subset(users_depl, !is.na(primer_pago))$usuarioid),collapse="','",sep = ""),
                           "')
                           and not (medio = 'mobiadvanced' and base_imponible = 6 and moneda = 'Eur')
                           and datediff(date(p.fecha),u.primer_pago)<=15
                           group by p.usuarioid
                           ;",sep = ""))

#View(bi_net_depl)




#xxxxxxxxxsxxxxxxxxxxxxxxxx#
#### * Number of medios ####
#xxxxxxxxxxsxxxxxxxxxxxxxxx#



num_medios_depl <- sqlQuery(conn, paste
                          ("select
                           p.usuarioid
                           , count(distinct(medio)) as num_medios
                           from pagosfacturas_view  p
                           inner join usuarios u on u.id=p.usuarioid
                           WHERE p.usuarioid in ('",
                           paste(unique(users_depl$usuarioid),collapse="','",sep = ""),
                           "')
                           and not (medio = 'mobiadvanced' and base_imponible = 6 and moneda = 'Eur')
                           and datediff(date(p.fecha),u.primer_pago)<=15
                           group by p.usuarioid
                           ;",sep = ""))



# head(num_medios_depl)

#xxxxxxxxxxxxxxxxxxxxxxxx#
#xxxxxxxxxxxxxxxxxxxxxxxx#
##### Game Behaviour #####
#xxxxxxxxxxxxxxxxxxxxxxxx#
#xxxxxxxxxxxxxxxxxxxxxxxx#






#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#
##### * Total games & time played before and after primer pago  #####
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#


games_time_depl <- sqlQuery(conn, paste
                       ("select 
                        l.usuario as usuarioid 
                        , count(distinct(l.id)) as times_played
                        , SUM(TIME_TO_SEC(tiempo)) as time_sec
                        , case when l.juego in (18, 41, 44, 48) then 'Bingo'
                          when l.juego in (27, 47, 32, 12, 20, 30, 34, 46) then 'Other_Casino'
                          when l.juego in (29, 8, 36, 28, 9, 42, 19, 17, 14, 11, 45, 26, 38, 33, 39, 5, 31, 37, 35) then 'Cartas'
                          when l.juego in (13, 10, 1, 25, 7, 3) then 'Mesa'
                          when l.juego in (21, 15, 2, 16, 22, 6, 4) then 'Habilidad'
                          else 'Desconocido' end as descripcion
                        , date(fecha) as fecha
                        , case when datediff(date(l.fecha),u.primer_pago)>=0 then 1 else 0 end as after_pp

                        from log_usuariostiempo l
                        inner join usuarios u on u.id=l.usuario

                        WHERE l.usuario in ('",paste(unique(subset(users_depl, !is.na(primer_pago))$usuarioid),collapse="','",sep = ""),"')
                        and datediff(date(l.fecha),u.primer_pago)<=15
                        group by l.usuario
                        , case when l.juego in (18, 41, 44, 48) then 'Bingo'
                          when l.juego in (27, 47, 32, 12, 20, 30, 34, 46) then 'Other_Casino'
                          when l.juego in (29, 8, 36, 28, 9, 42, 19, 17, 14, 11, 45, 26, 38, 33, 39, 5, 31, 37, 35) then 'Cartas'
                          when l.juego in (13, 10, 1, 25, 7, 3) then 'Mesa'
                          when l.juego in (21, 15, 2, 16, 22, 6, 4) then 'Habilidad'
                          else 'Desconocido' end
                        , date(fecha)
                        , case when datediff(date(l.fecha),u.primer_pago)>=0 then 1 else 0 end
                        ;",sep = ""))



# head(games_time_depl)

####  *  * Days Retained ####

games_time_depl.retention<-sqldf("select count(distinct(fecha)) as days_retained
                            , usuarioid
                            , case when after_pp=1 then 'retained_app' else 'retained_bpp' end as b_a
                            from 'games_time_depl'
                            group by
                            usuarioid
                            , case when after_pp=1 then 'retained_app' else 'retained_bpp' end")

#View(games_time.retention)

retention_depl<-dcast(games_time_depl.retention
                , usuarioid ~ b_a
                , value.var="days_retained"
                , fun.aggregate=sum)

#View(retention)

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#
####  * * Partidas per game - before and after primer pago ####
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#


games_time_depl.to_partidas<- sqldf("select
                                usuarioid 
                               , sum(time_sec) as time_sec
                               , sum(times_played) as times_played
                               , descripcion
                               , case when after_pp=1 then 'app' else 'bpp' end as a_b
                               from 'games_time_depl'
                               where time_sec>0
                               group by usuarioid, descripcion, case when after_pp=1 then 'app' else 'bpp' end")



partidas_depl <- dcast(games_time_depl.to_partidas
                , usuarioid ~ descripcion+a_b
                , value.var="times_played"
                , fun.aggregate=sum)


# head(partidas_depl)

for (i in 2:length(partidas_depl)) {
  colnames(partidas_depl)[i]<-paste("partidas_",colnames(partidas_depl)[i],sep="")
}

# Total partidas played first 15 days
#partidas.app$total_partidas<-rowSums(partidas[,2:length(partidas)])








#xxxxxxxxxxxxxxxxxxxxxxxxxxx#
####  * * Time per game  ####
#xxxxxxxxxxxxxxxxxxxxxxxxxxx#




time_depl <- dcast(games_time_depl.to_partidas
                  , usuarioid ~ descripcion+a_b
                  , value.var="time_sec"
                  , fun.aggregate=sum)



# Total partidas played first 15 days
#time$total_partidas<-rowSums(time[,2:38])

#View(time)




# Add time_  before each var
for (i in 2:length(time_depl)) {
  colnames(time_depl)[i]<-paste("time_",colnames(time_depl)[i],sep="")
}
















#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#
####  * Pull Chips bet, chips resume  ####
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#



chips_transact_depl <- sqlQuery(conn, paste
                   ("select
                    s.user_id as usuarioid
                    , sum(s.chips_bet) AS chips_bet
                    , sum(s.chips_resume) AS chips_res
                    , case when s.game_id in (18, 41, 44, 48) then 'Bingo'
                          when s.game_id in (27, 47, 32, 12, 20, 30, 34, 46) then 'Other_Casino'
                          when s.game_id in (29, 8, 36, 28, 9, 42, 19, 17, 14, 11, 45, 26, 38, 33, 39, 5, 31, 37, 35) then 'Cartas'
                          when s.game_id in (13, 10, 1, 25, 7, 3) then 'Mesa'
                          when s.game_id in (21, 15, 2, 16, 22, 6, 4) then 'Habilidad'
                          else 'Desconocido' end as descripcion
                    from segment_chips_day_user_game  s
                    inner join usuarios u on u.id=s.user_id
                    WHERE s.user_id in ('",
                    paste(unique(subset(users_depl, !is.na(primer_pago))$usuarioid),collapse="','",sep = ""),
                    "')
                    and datediff(date(s.date_played),u.primer_pago)<=15
                    and datediff(date(s.date_played),u.primer_pago)>=0
                    group by s.user_id
                    , case when s.game_id in (18, 41, 44, 48) then 'Bingo'
                          when s.game_id in (27, 47, 32, 12, 20, 30, 34, 46) then 'Other_Casino'
                          when s.game_id in (29, 8, 36, 28, 9, 42, 19, 17, 14, 11, 45, 26, 38, 33, 39, 5, 31, 37, 35) then 'Cartas'
                          when s.game_id in (13, 10, 1, 25, 7, 3) then 'Mesa'
                          when s.game_id in (21, 15, 2, 16, 22, 6, 4) then 'Habilidad'
                          else 'Desconocido' end
                    ;",sep = ""))


#View(chips_transact)

chips_transact_depl$chips_res<-as.numeric(chips_transact_depl$chips_res)
chips_transact_depl$chips_bet<-as.numeric(chips_transact_depl$chips_bet)


#Attach "_CR" for "Chips Resume" at end of each game
#chips_transact_depl$descripcion<-paste(chips_transact_depl$descripcion,"_CR",sep="")







chips_resume_depl <- dcast(chips_transact_depl
                  , usuarioid ~ descripcion
                  , value.var="chips_bet"
                  , fun.aggregate=sum)


#head(chips_resume_depl)



# Add ChBet_  before each var
for (i in 2:length(chips_resume_depl)) {
  colnames(chips_resume_depl)[i]<-paste("ChBet_",colnames(chips_resume_depl)[i],sep="")
}












#xxxxxxxxxxxxxxxxxxx#
#### Final Table ####
#xxxxxxxxxxxxxxxxxxx#


final_depl<-sqldf("select u.usuarioid
                   , u.primer_pago
                   , u.fecha_alta

                   , case when u.reg_country in ('ES','IT','FR','BR','AR') then u.reg_country else 'Other' end as reg_country

                   , case when u.company = 'Akamon' then 1 else 0 end as reg_akamon

                   , case when u.plataforma=0 then 'Portal' when u.plataforma=1 then 'Facebook'
                    when u.plataforma in (2,3) then 'Mobile' else 'Other' end as platform

                   , case when u.juego_usu in ('Billar','BuscaTesoros','Cuadrox','Minigolf','Superbuteo','Tetriwar') then 'Habilidad'
                   when u.juego_usu in ('Aljedrez','Damas','Domino','GuerraNaval','PalabrasCruzadas','Parchis','Pictiomatic') then 'Mesa'
                   when u.juego_usu in ('Truco','Truco argentino','Truco paulista/mineiro') then 'Truco'
                   when u.juego_usu in ('Fortuna') then 'Other'
                   else u.juego_usu end as juego_usu
                   ,u.acq_juego_category


                   , bn.bi as x_bi
                   , bn.nr as x_nr
                   , bn.ATV_bi as x_ATV_bi
                   , bn.ATV_nr as x_ATV_nr


                   , nm.num_medios
                   , u.pp_rec
                   , ret.retained_app
                   , ret.retained_bpp/u.pp_rec as retained_bpp

                    
                    , p.partidas_Bingo_app
                    , p.partidas_Cartas_app
                    , p.partidas_Habilidad_app
                    , p.partidas_Mesa_app
                    , p.partidas_Other_Casino_app

                    , p.partidas_Bingo_bpp
                    , p.partidas_Cartas_bpp
                    , p.partidas_Habilidad_bpp
                    , p.partidas_Mesa_bpp
                    , p.partidas_Other_Casino_bpp


                    , round(cr.ChBet_Bingo/tm.time_Bingo_app, 8) as Total_Bingo_Chips_cps
                    , round(cr.ChBet_Other_Casino/tm.time_Other_Casino_app, 8)  as Total_Casino_Chips_cps
                    , round(cr.ChBet_Cartas/tm.time_Cartas_app, 8)  as Total_Cartas_Chips_cps
                    , round(cr.ChBet_Mesa/tm.time_Mesa_app, 8)  as Total_Mesa_Chips_cps
                    , round(cr.ChBet_Habilidad/tm.time_Habilidad_app, 8)  as Total_Habilidad_Chips_cps


                    , cr.ChBet_Bingo
                    , cr.ChBet_Cartas
                    , cr.ChBet_Other_Casino
                    , cr.ChBet_Mesa
                    , cr.ChBet_Habilidad



                   , tm.time_Bingo_app
                   , tm.time_Other_Casino_app
                   , tm.time_Cartas_app
                   , tm.time_Mesa_app
                   , tm.time_Habilidad_app

                   , (tm.time_Bingo_bpp+tm.time_Other_Casino_bpp+ tm.time_Cartas_bpp+ tm.time_Mesa_bpp+ tm.time_Habilidad_bpp)/u.pp_rec as total_time_bpp




             from 'users_depl' as u
             inner join 'bi_net_depl' as bn on bn.usuarioid=u.usuarioid
             inner join 'partidas_depl' as p on p.usuarioid=u.usuarioid
             left join 'time_depl' as tm on tm.usuarioid=u.usuarioid
             left join 'chips_resume_depl' as cr on cr.usuarioid=u.usuarioid
             left join 'retention_depl' ret on ret.usuarioid=u.usuarioid
             left join 'num_medios_depl' nm on nm.usuarioid=u.usuarioid
             ")





final_depl[is.na(final_depl)] <- 0



# final_depl<-read.csv(file = "C://Users//nikolay.nenov//Desktop//LTV.csv", sep=";")



final_depl$reg_country          <-as.factor(final_depl$reg_country)
#final_depl$company             <-as.factor(final_depl$company)
final_depl$platform            <-as.factor(final_depl$platform)
final_depl$juego_usu           <-as.factor(final_depl$juego_usu)
final_depl$acq_juego_category  <-as.factor(final_depl$acq_juego_category)

#final_depl$x_nr      <-(as.numeric(final_depl$x_nr)-mean(as.numeric(final_depl$x_nr),na.rm=T))/sd(as.numeric(final_depl$x_nr),na.rm=T)
#final_depl$x_ATV_bi  <-(as.numeric(final_depl$x_ATV_bi)-mean(as.numeric(final_depl$x_ATV_bi),na.rm=T))/sd(as.numeric(final_depl$x_ATV_bi),na.rm=T)


final_depl$num_medios<-as.numeric(final_depl$num_medios)
final_depl$pp_rec    <-(as.numeric(final_depl$pp_rec)-mean(as.numeric(final_depl$pp_rec),na.rm=T))/sd(as.numeric(final_depl$pp_rec),na.rm=T)
#final$retained_app    <-(as.numeric(final$retained_app)-mean(as.numeric(final$retained_app),na.rm=T))/sd(as.numeric(final$retained_app),na.rm=T)
final_depl$retained_bpp    <-(as.numeric(final_depl$retained_bpp)-mean(as.numeric(final_depl$retained_bpp),na.rm=T))/sd(as.numeric(final_depl$retained_bpp),na.rm=T)



## Partidas ##

final_depl$partidas_Bingo_app        <-(as.numeric(final_depl$partidas_Bingo_app)-mean(as.numeric(final_depl$partidas_Bingo_app),na.rm=T))/sd(as.numeric(final_depl$partidas_Bingo_app),na.rm=T)
final_depl$partidas_Cartas_app       <-(as.numeric(final_depl$partidas_Cartas_app)-mean(as.numeric(final_depl$partidas_Cartas_app),na.rm=T))/sd(as.numeric(final_depl$partidas_Cartas_app),na.rm=T)
final_depl$partidas_Habilidad_app    <-(as.numeric(final_depl$partidas_Habilidad_app)-mean(as.numeric(final_depl$partidas_Habilidad_app),na.rm=T))/sd(as.numeric(final_depl$partidas_Habilidad_app),na.rm=T)
final_depl$partidas_Mesa_app         <-(as.numeric(final_depl$partidas_Mesa_app)-mean(as.numeric(final_depl$partidas_Mesa_app),na.rm=T))/sd(as.numeric(final_depl$partidas_Mesa_app),na.rm=T)
final_depl$partidas_Other_Casino_app <-(as.numeric(final_depl$partidas_Other_Casino_app)-mean(as.numeric(final_depl$partidas_Other_Casino_app),na.rm=T))/sd(as.numeric(final_depl$partidas_Other_Casino_app),na.rm=T)

# final_depl$partidas_Bingo_bpp        <-(as.numeric(final_depl$partidas_Bingo_bpp)-mean(as.numeric(final_depl$partidas_Bingo_bpp),na.rm=T))/sd(as.numeric(final_depl$partidas_Bingo_bpp),na.rm=T)
# final_depl$partidas_Cartas_bpp       <-(as.numeric(final_depl$partidas_Cartas_bpp)-mean(as.numeric(final_depl$partidas_Cartas_bpp),na.rm=T))/sd(as.numeric(final_depl$partidas_Cartas_bpp),na.rm=T)
# final_depl$partidas_Habilidad_bpp    <-(as.numeric(final_depl$partidas_Habilidad_bpp)-mean(as.numeric(final_depl$partidas_Habilidad_bpp),na.rm=T))/sd(as.numeric(final_depl$partidas_Habilidad_bpp),na.rm=T)
# final_depl$partidas_Mesa_bpp         <-(as.numeric(final_depl$partidas_Mesa_bpp)-mean(as.numeric(final_depl$partidas_Mesa_bpp),na.rm=T))/sd(as.numeric(final_depl$partidas_Mesa_bpp),na.rm=T)
# final_depl$partidas_Other_Casino_bpp <-(as.numeric(final_depl$partidas_Other_Casino_bpp)-mean(as.numeric(final_depl$partidas_Other_Casino_bpp),na.rm=T))/sd(as.numeric(final_depl$partidas_Other_Casino_bpp),na.rm=T)

## Chips ##

final_depl$Total_Bingo_Chips_cps     <-(as.numeric(final_depl$Total_Bingo_Chips_cps)-mean(as.numeric(final_depl$Total_Bingo_Chips_cps),na.rm=T))/sd(as.numeric(final_depl$Total_Bingo_Chips_cps),na.rm=T)
final_depl$Total_Casino_Chips_cps    <-(as.numeric(final_depl$Total_Casino_Chips_cps)-mean(as.numeric(final_depl$Total_Casino_Chips_cps),na.rm=T))/sd(as.numeric(final_depl$Total_Casino_Chips_cps),na.rm=T)
final_depl$Total_Cartas_Chips_cps    <-(as.numeric(final_depl$Total_Cartas_Chips_cps)-mean(as.numeric(final_depl$Total_Cartas_Chips_cps),na.rm=T))/sd(as.numeric(final_depl$Total_Cartas_Chips_cps),na.rm=T)
final_depl$Total_Mesa_Chips_cps      <-(as.numeric(final_depl$Total_Mesa_Chips_cps)-mean(as.numeric(final_depl$Total_Mesa_Chips_cps),na.rm=T))/sd(as.numeric(final_depl$Total_Mesa_Chips_cps),na.rm=T)
final_depl$Total_Habilidad_Chips_cps <-(as.numeric(final_depl$Total_Habilidad_Chips_cps)-mean(as.numeric(final_depl$Total_Habilidad_Chips_cps),na.rm=T))/sd(as.numeric(final_depl$Total_Habilidad_Chips_cps),na.rm=T)


final_depl$ChBet_Bingo         <-(as.numeric(final_depl$ChBet_Bingo)-mean(as.numeric(final_depl$ChBet_Bingo),na.rm=T))/sd(as.numeric(final_depl$ChBet_Bingo),na.rm=T)
final_depl$ChBet_Other_Casino  <-(as.numeric(final_depl$ChBet_Other_Casino)-mean(as.numeric(final_depl$ChBet_Other_Casino),na.rm=T))/sd(as.numeric(final_depl$ChBet_Other_Casino),na.rm=T)
final_depl$ChBet_Cartas        <-(as.numeric(final_depl$ChBet_Cartas)-mean(as.numeric(final_depl$ChBet_Cartas),na.rm=T))/sd(as.numeric(final_depl$ChBet_Cartas),na.rm=T)
final_depl$ChBet_Mesa          <-(as.numeric(final_depl$ChBet_Mesa)-mean(as.numeric(final_depl$ChBet_Mesa),na.rm=T))/sd(as.numeric(final_depl$ChBet_Mesa),na.rm=T)
final_depl$ChBet_Habilidad     <-(as.numeric(final_depl$ChBet_Habilidad)-mean(as.numeric(final_depl$ChBet_Habilidad),na.rm=T))/sd(as.numeric(final_depl$ChBet_Habilidad),na.rm=T)

## Time ##

final_depl$time_Bingo_app          <-(as.numeric(final_depl$time_Bingo_app)-mean(as.numeric(final_depl$time_Bingo_app),na.rm=T))/sd(as.numeric(final_depl$time_Bingo_app),na.rm=T)
final_depl$time_Other_Casino_app   <-(as.numeric(final_depl$time_Other_Casino_app)-mean(as.numeric(final_depl$time_Other_Casino_app),na.rm=T))/sd(as.numeric(final_depl$time_Other_Casino_app),na.rm=T)
final_depl$time_Cartas_app         <-(as.numeric(final_depl$time_Cartas_app)-mean(as.numeric(final_depl$time_Cartas_app),na.rm=T))/sd(as.numeric(final_depl$time_Cartas_app),na.rm=T)
final_depl$time_Mesa_app           <-(as.numeric(final_depl$time_Mesa_app)-mean(as.numeric(final_depl$time_Mesa_app),na.rm=T))/sd(as.numeric(final_depl$time_Mesa_app),na.rm=T)
final_depl$time_Habilidad_app      <-(as.numeric(final_depl$time_Habilidad_app)-mean(as.numeric(final_depl$time_Habilidad_app),na.rm=T))/sd(as.numeric(final_depl$time_Habilidad_app),na.rm=T)

final_depl$total_time_bpp          <-(as.numeric(final_depl$total_time_bpp)-mean(as.numeric(final_depl$total_time_bpp),na.rm=T))/sd(as.numeric(final_depl$total_time_bpp),na.rm=T)





ping(1)


#str(final_depl)
#colnames(final_depl)

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#

# PPPP   RRRR   EEEEE  DDDD    I    CCCC  TTTTT  I    OOO   NN   N
# P   P  R   R  E      D   D   I   C        T    I   O   O  N N  N
# PPPP   RRRR   EEE    D   D   I   C        T    I   O   O  N  N N
# P      R R    E      D   D   I   C        T    I   O   O  N   NN
# P      R  R   EEEEE  DDDD    I    CCCC    T    I    OOO   N    N

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#



# final_depl$y_bi_range <- predict(rf2, newdata=final_depl)
#View(table(final_depl$y_bi_range))

final_depl$y_bi_range_wtd <- predict(rf3, newdata=final_depl)
#View(table(final_depl$y_bi_range_wtd))







#### Assign  average LTV per each user within a cohort ####

# final_depl$ltv<-ifelse(final_depl$y_bi_range=='000-005', (1.214224*2.36),
#                        ifelse(final_depl$y_bi_range=='005-030', (11.918941*2.36),
#                               ifelse(final_depl$y_bi_range=='030-070', (45.630304*2.36),
#                                      ifelse(final_depl$y_bi_range=='070+', (346.257104*2.36),NA)
#                        )))


final_depl$ltv<-ifelse(final_depl$y_bi_range_wtd=='000-005', (1.214224*2.36),
                       ifelse(final_depl$y_bi_range_wtd=='005-030', (17.445181*2.36),
                              ifelse(final_depl$y_bi_range_wtd=='030-070', (17.445181*2.36),
                                     ifelse(final_depl$y_bi_range_wtd=='070+', (346.257104*2.36),NA)
                       )))



# What portion of LTV has not been covered yet

final_depl$value_not_covered<-final_depl$ltv-2*final_depl$x_nr

# head(final_depl)


## Export in CSV

# write.table(final_depl[,c(1,2,4,6,45)], file = "C://Users//nikolay.nenov//Desktop//LTV.csv", sep=";", row.names =F)

# str(final_depl[,c(1,44)])

sqlSave(conn, final_depl[,c(1,44,45)], tablename = "estadisticas.usu_LTV"
        , rownames=F, safer=F, fast=F, append=T
        , varTypes=c(usuarioid="int(11)"
                     , ltv="DECIMAL(20,10)"
                     , value_not_covered="DECIMAL(20,10)")
        )




#### This part clusters users ####



cust2target<-read.csv(file="C://Users//nikolay.nenov//Documents//Projects//Temp//test_users.csv", sep=",",header=TRUE);
str(cust2target)

colnames(final_depl)

final_depl.subs<-(subset(final_depl, usuarioid %in% unique(cust2target$user_id))[,c(1,10,12:14,17:21,32:36)])



test.dist<-dist(as.matrix(final_depl.subs), method = "euclidean")


test.clust<-hclust(test.dist, method="ward.D")


plot(test.clust)


test.groups<-cutree(test.clust, k=3)


rect.hclust(test.clust, k=3, border="red")



final_depl.subs$cluster<-test.groups


table(final_depl.subs$cluster)



library(doBy)
summaryBy( .  ~ cluster, data = final_depl.subs, FUN = mean )






final_depl.subs2<-subset(final_depl.subs,cluster==1)


test.dist2<-dist(as.matrix(final_depl.subs2), method = "euclidean")


test.clust2<-hclust(test.dist2, method="ward.D")


plot(test.clust)


test.groups2<-cutree(test.clust2, k=4)

# head(test.groups2)

rect.hclust(test.clust2, k=4, border="red")



final_depl.subs2$cluster<-test.groups2


table(final_depl.subs2$cluster)




### Need to add usuarioid first!!!
final_depl.subs<-sqldf("select a.*
                       , b.cluster as subcluster
                       from 'final_depl.subs' a
                       left join 'final_depl.subs2' b on a.usuarioid=b.usuarioid")

table(final_depl.subs2$cluster)


library(doBy)
View(summaryBy( .  ~ cluster + subcluster, data = final_depl.subs, FUN = mean ))


final_depl.subs$subcluster<-ifelse(is.na(final_depl.subs$subcluster),0,final_depl.subs$subcluster)

View(mutate(final_depl.subs, Segment=paste(cluster,'.',subcluster,sep=''))[,c(1,18)])

