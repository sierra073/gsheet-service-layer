## Clearing memory
rm(list=ls())

## load packages (if not already in the environment)
packages.to.install <- c("DBI", "rJava", "RJDBC", "dotenv","ggplot2")
for (i in 1:length(packages.to.install)){
  if (!packages.to.install[i] %in% rownames(installed.packages())){
    install.packages(packages.to.install[i])
  }
}
library(DBI)
library(rJava)
library(RJDBC)
library(dotenv)
library(ggplot2)
options(java.parameters = "-Xmx4g" )

## source environment variables

assign("GITHUB", Sys.getenv("GITHUB"), envir=.GlobalEnv)
assign("USER_DAR", Sys.getenv("USER_DAR"), envir=.GlobalEnv)
assign("PASSWORD_DAR", Sys.getenv("PASSWORD_DAR"), envir=.GlobalEnv)
assign("URL_DAR", Sys.getenv("URL_DAR"), envir=.GlobalEnv)

##**************************************************************************************************************************************************
## QUERY THE DB

## load PostgreSQL Driver
pgsql <- JDBC("org.postgresql.Driver", paste(GITHUB, "/General_Resources/postgres_driver/postgresql-9.4.1212.jre7.jar", sep=""), "`")

## connect to the database
con <- dbConnect(pgsql, url=URL_DAR, user=USER_DAR, password=PASSWORD_DAR)

## query function
querydb <- function(query_name){
  query <- readChar(query_name, file.info(query_name)$size)
  data <- dbGetQuery(con, query)
  return(data)
}

data <- querydb(paste(GITHUB, "/''scripts/id5023_no_cost_decrease_pct_2018sots.sql", sep=""))
data2 <- querydb(paste(GITHUB, "/''scripts/id5024_no_cost_decrease_no_aff_pct_2018sots.sql", sep=""))

## disconnect from database
dbDisconnect(con)

# The doughnut function permits to draw a donut plot
doughnut <-
function (x, labels = names(x), edges = 200, outer.radius = 0.8, 
          inner.radius=0.6, clockwise = FALSE,
          init.angle = if (clockwise) 90 else 0, density = NULL, 
          angle = 45, col = NULL, border = FALSE, lty = NULL, 
          main = NULL, ...)
{
    if (!is.numeric(x) || any(is.na(x) | x < 0))
        stop("'x' values must be positive.")
    if (is.null(labels))
        labels <- as.character(seq_along(x))
    else labels <- as.graphicsAnnot(labels)
    x <- c(0, cumsum(x)/sum(x))
    dx <- diff(x)
    nx <- length(dx)
    plot.new()
    pin <- par("pin")
    xlim <- ylim <- c(-1, 1)
    if (pin[1L] > pin[2L])
        xlim <- (pin[1L]/pin[2L]) * xlim
    else ylim <- (pin[2L]/pin[1L]) * ylim
    plot.window(xlim, ylim, "", asp = 1)
    if (is.null(col))
        col <- if (is.null(density))
          palette()
        else par("fg")
    col <- rep(col, length.out = nx)
    border <- rep(border, length.out = nx)
    lty <- rep(lty, length.out = nx)
    angle <- rep(angle, length.out = nx)
    density <- rep(density, length.out = nx)
    twopi <- if (clockwise)
        -2 * pi
    else 2 * pi
    t2xy <- function(t, radius) {
        t2p <- twopi * t + init.angle * pi/180
        list(x = radius * cos(t2p), 
             y = radius * sin(t2p))
    }
    for (i in 1L:nx) {
        n <- max(2, floor(edges * dx[i]))
        P <- t2xy(seq.int(x[i], x[i + 1], length.out = n),
                  outer.radius)
        polygon(c(P$x, 0), c(P$y, 0), density = density[i], 
                angle = angle[i], border = border[i], 
                col = col[i], lty = lty[i])
        Pout <- t2xy(mean(x[i + 0:1]), outer.radius)
        lab <- as.character(labels[i])
        if (!is.na(lab) && nzchar(lab)) {
            lines(c(1, 1.05) * Pout$x, c(1, 1.05) * Pout$y)
            text(1.1 * Pout$x, 1.1 * Pout$y, labels[i], 
                 xpd = TRUE, adj = ifelse(Pout$x < 0, 1, 0), cex=.7, 
                 ...)
        }
        ## Add white disc          
        Pin <- t2xy(seq.int(0, 1, length.out = n*nx),
                  inner.radius)
        polygon(Pin$x, Pin$y, density = density[i], 
                angle = angle[i], border = border[i], 
                col = "white", lty = lty[i])
    }
 
    title(main = main, ...)
    invisible(NULL)
}

doughnut(c(100 - round(data$num[2]*100,0), round(data$num[2]*100,0)),labels = c('Cost Decrease', 'No Cost Decrease'), init.angle = 166, outer.radius = 1, inner.radius=0.7, col=c('#2b7bba','darkgrey') )
doughnut(c(100 - round(data$num[3]*100,0), round(data$num[3]*100,0)),labels = c('No peer deal', 'Have peer deal'), init.angle = 39, outer.radius = 1, inner.radius=0.7, col=c('lightgrey','orange') )
doughnut(c(round(data2$num[2]*100,0),c(100 - round(data2$num[2]*100,0))),labels = c('Not meeting benchmark prices','Meeting benchmark prices'), init.angle = 10, outer.radius = 1, inner.radius=0.7, col=c('#da3b46','lightgrey') )
