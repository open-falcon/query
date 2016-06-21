aliasName = (typeof aliasName == "undefined"? "SummaryResult" : aliasName)
sum_all = _.reduceRight(input, function(list, records){
  if(_.isEmpty(list)){
    list = records
    list.Endpoint = aliasName
  }
  _.each(records.Values, function(v,indx){
    list.Values[indx].Value += v.Value
  })
  return list
},{})
output = JSON.stringify(sum_all)
