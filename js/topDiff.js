/*
  get diff value of each 2 point and retrun top n
*/
limit = (typeof limit == "undefined"? 3 : limit)
orderby = (typeof orderby == "undefined"? "desc" : orderby)
sortby = (typeof sortby == "undefined"? "Mean" : sortby)
t2 = _.map(input, function(res){
  res.MaxInc = 0
  if( res.Values.length == 0){
    return res
  }else{
    diffvalues = []
    flag = 0
    _.reduce(res.Values, function(lastVal,v){
      value = (isNaN(v.Value)? 0 : v.Value)
      if(flag === 0){
        lastVal = value
      }else{
        restmp = lastVal - value
        diffvalues.push(restmp)
        lastVal = value
      }
      flag++
      return lastVal
    },0)
    mean = _.reduce(diffvalues, function(sum,v){
      return (sum+v)
    },0) / (diffvalues.length === 0 ? 1 : diffvalues.length)
    res.Mean = Math.round(mean,0)
    res.Max = Math.round(_.max(diffvalues), 0)
    res.Min = Math.round(_.min(diffvalues), 0)
    return res
  }
})

t3 = _.chain(t2).sortBy(function(res){

  if(orderby == "desc"){
    return - res[sortby]
  }else{
    return res[sortby]
  }

}).first(limit).value()

output = JSON.stringify(t3)
