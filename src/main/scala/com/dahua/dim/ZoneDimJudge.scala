package com.dahua.dim

/**
 * 地域分析指标判断
 */
object ZoneDimJudge {

  /**
   * 前三个指标公用的判断,如果符合第三个维度的判断，那么他就满足原始请求，有效请求，广告请求，这是三个唯独
   * 如果只是满足第一个的话 那么就不满足其它的请求，自然就是0
   *
   * @param requestMode
   * @param processNode
   */
  def qingQiuJudge(requestMode: Int, processNode: Int) = {

    if (requestMode == 1 && processNode >= 1) {
      List[Double](1, 0, 0)
    } else if (requestMode == 1 && processNode >= 1) {
      List[Double](1, 1, 0)
    } else if (requestMode == 1 && processNode == 1) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }


  /**
   * 参与竞价的判断
   *
   * @param ecTive
   * @param Bill
   * @param bid
   * @param orderId
   */
  def canYuJingJiaJudge(adppi: Int, ecTive: Int, Bill: Int, bid: Int, orderId: Int) = {
    if (adppi >= 100000 & ecTive == 1 && Bill == 1 && bid == 1 && orderId != 0) {
      List[Double](1)
    } else {
      List[Double](0)
    }
  }

  /**
   * 成功竞价的判断
   *
   * @param ecTive
   * @param Bill
   * @param bid
   * @param isWin
   */
  def chengGongJingJiaJudge(adppi: Int, ecTive: Int, Bill: Int, bid: Int, isWin: Int) = {
    if (adppi >= 100000 & ecTive == 1 && Bill == 1 && bid == 1 && isWin == 1) {
      List[Double](1)
    } else {
      List[Double](0)
    }
  }

  /**
   * 广告展示数量与广告点击数判断
   *
   * @param rMode
   * @param ecTive
   */
  def guangGaoZhanShi(rMode: Int, ecTive: Int) = {
    if (rMode == 2 & ecTive == 1) {
      List[Double](1, 0)
    } else if (rMode == 3 & ecTive == 1) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }
  }

  /**
   * 媒介展示数与点击数
   *
   * @param rMode
   * @param ecTive
   * @param Bill
   * @return
   */
  def meiJieZhanShi(rMode: Int, ecTive: Int, Bill: Int) = {
    if (rMode == 2 & ecTive == 1 & Bill == 1) {
      List[Double](1, 0)
    }
    else if (rMode == 3 & ecTive == 1 & Bill == 1) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }
  }

  /**
   * DSP的消费与成本
   */
  def DSPXiaoFei(adppi: Int, ecTive: Int, Bill: Int, isWin: Int, adorderID: Int, adcreativeID: Int, winPrice: Double, adPatyment: Double) = {
    if (adppi >= 100000 & ecTive == 1 && Bill == 1 && isWin == 1 & adorderID >= 200000 & adcreativeID >= 200000) {
      List[Double](winPrice * 1.0 / 1000, adPatyment * 1.0 / 1000)
    } else {
      List[Double](0, 0)
    }
  }


}
