/*
  get avg value of counter and retrun top n
*/
limit = (typeof limit == "undefined"? 3 : limit)
orderby = (typeof orderby == "undefined"? "desc" : orderby)
sortby = (typeof sortby == "undefined"? "Mean" : sortby)
t2 = _.map(input, function(res){
  res.Mean = 0
  res.Max = 0
  res.Min = 0
  if( res.Values.length == 0){
    return res
  }else{
    values = []
    mean = _.reduce(res.Values, function(sum,v){
      value = (isNaN(v.Value)? 0 : v.Value)
      values.push(value)
      return (sum+value)
    },0) / (res.Values.length === 0 ? 1 : res.Values.length)
    res.Mean = Math.round(mean,0)
    res.Max = Math.round(_.max(values), 0)
    res.Min = Math.round(_.min(values), 0)
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
