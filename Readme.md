学习kfc peta client 服务



object_Counter.h
ObjectCounter的主要功能是统计某种类型对象的数量。它通过原子变量 count 实现线程安全的计数。具体功能如下：
Get()：每当获取一个对象时，计数加一。
Reset()：每当归还一个对象时，计数减一。
Count()：返回当前对象的数量。
在 get_object() 和 return_object() 这两个辅助函数中，分别在获取和归还对象时调用计数器的方法，实现自动统计对