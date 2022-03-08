#!/bin/bash
echo "SH"

 curl 'http://query.sse.com.cn//search/getSearchResult.do?search=qwjs&jsonCallBack=jQuery112408335483635979806_1596531148390&page=1&searchword=T_L+CTITLE+T_D+E_KEYWORDS+T_JT_E+T_L%E5%85%B3%E4%BA%8E%E5%85%AC%E5%BC%80%E5%8F%91%E8%A1%8C%E5%8F%AF%E8%BD%AC%E6%8D%A2%E5%85%AC%E5%8F%B8%E5%80%BA%E5%88%B8%E7%94%B3%E8%AF%B7%E8%8E%B7%E5%BE%97T_R++and+cchannelcode+T_E+T_L0T_D8311T_D8348T_D8349T_D8365T_D8415T_D88888888T_DT_RT_R&orderby=-CRELEASETIME&perpage=10&_=1596531148411' \
  -H 'Proxy-Connection: keep-alive' \
  -H 'Pragma: no-cache' \
  -H 'Cache-Control: no-cache' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36' \
  -H 'DNT: 1' \
  -H 'Accept: */*' \
  -H 'Referer: http://www.sse.com.cn/home/search/?webswd=%E5%8F%AF%E8%BD%AC%E5%80%BA,%E5%AE%A1%E6%A0%B8%E9%80%9A%E8%BF%87' \
  -H 'Accept-Language: en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6,ja;q=0.5' \
  -H 'Cookie: yfx_c_g_u_id_10000042=_ck20042823343016351284756398665; VISITED_MENU=%5B%229059%22%2C%229056%22%2C%228824%22%2C%228528%22%2C%229062%22%2C%228535%22%2C%228505%22%2C%228307%22%5D; yfx_f_l_v_t_10000042=f_t_1588088070624__r_t_1596531029697__v_t_1596531147782__r_c_3; seecookie=%u53EF%u8F6C%u503A%20%u5BA1%u6838%2C%u53EF%u8F6C%u503A%20%u5BA1%u6838%u901A%u8FC7%2C%u5173%u4E8E%u516C%u5F00%u53D1%u884C%u53EF%u8F6C%u6362%u516C%u53F8%u503A%u5238%u7533%u8BF7%u83B7%u5F97' \
  --compressed \
  --insecure

echo "\n\n"
echo "SZ"

curl 'http://www.szse.cn/api/disc/announcement/annList?random=0.6347070981819911' \
  -H 'Proxy-Connection: keep-alive' \
  -H 'Pragma: no-cache' \
  -H 'Cache-Control: no-cache' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'X-Request-Type: ajax' \
  -H 'DNT: 1' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36' \
  -H 'Content-Type: application/json' \
  -H 'Origin: http://www.szse.cn' \
  -H 'Referer: http://www.szse.cn/disclosure/listed/notice/index.html' \
  -H 'Accept-Language: en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6,ja;q=0.5' \
  --data-binary '{"seDate":["2020-08-04","2020-08-04"],"searchKey":["关于公开发行可转换公司债券申请获得中国证券监督管理"],"channelCode":["listedNotice_disc"],"pageSize":30,"pageNum":1}' \
  --compressed \
  --insecure
