#### function to calculate mrc using knapsack method

### input is bandwidth and the output is the budget

### f_method indicates which costs should be used: 
## (1) "median" - uses the 2019 median of prices for Fiber internet circuits
## (2) "30th_percentile" - uses the 2019 30th percentile of prices for Fiber internet circuits
## (3) "knapsack" - uses the classic 2015 30th percentile of prices for Fiber internet circuits 

f_knapsack <- function(bandwidth,f_method) {
  func_table <- knapsack[(knapsack$method==f_method),]
  budget <- 0
  while (bandwidth > 0) {
    if (bandwidth >= 10000){
      bandwidth <- bandwidth - 10000
      budget <- budget + func_table[(func_table$bandwidth_in_mbps == 10000),"cost"]
    } else if (bandwidth >= 1000){
      bandwidth <- bandwidth - 1000
      budget <- budget + func_table[(func_table$bandwidth_in_mbps == 1000),"cost"]
    } else if (bandwidth >= 500){
      bandwidth <- bandwidth - 500
      budget <- budget + func_table[(func_table$bandwidth_in_mbps == 500),"cost"]
    } else if (bandwidth >= 200){
      bandwidth <- bandwidth - 200
      budget <- budget + func_table[(func_table$bandwidth_in_mbps == 200),"cost"]
    } else if (bandwidth >= 100){
      bandwidth <- bandwidth - 100
      budget <- budget + func_table[(func_table$bandwidth_in_mbps == 100),"cost"]
    } else if (bandwidth < 100){
      bandwidth <- 0
      budget <- budget + func_table[(func_table$bandwidth_in_mbps == 50),"cost"]
    }
  }
  
  return(budget)
  
} 
