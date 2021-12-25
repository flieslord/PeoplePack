request = function()
    uid = math.random(1, 10000000)
    path = "/matchCrowd?uid=" .. uid .. '&cid=c0001'
    return wrk.format(nil, path)
 end