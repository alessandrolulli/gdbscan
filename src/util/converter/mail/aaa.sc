package util.converter.mail

object aaa {
  val t = new OrderingMail                        //> t  : util.converter.mail.OrderingMail = util.converter.mail.OrderingMail@4b2b
                                                  //| 7414
  
  val l = ("aaa", 1) :: ("bbb", 3) :: ("ccc", 2) :: Nil
                                                  //> l  : List[(String, Int)] = List((aaa,1), (bbb,3), (ccc,2))
  
  l.sorted(t)                                     //> res0: List[(String, Int)] = List((bbb,3), (ccc,2), (aaa,1))
  
}