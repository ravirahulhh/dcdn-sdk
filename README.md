# DCDN sdk develop

# 开发规范

## 跨平台
- 只使用C++17语言标准和依赖库封装好的接口

## 命名规范
- 合理使用命名空间
- 类名和public函数用首字母大写驼峰式，protected和private函数用首字母小写驼峰式
- 私有成员变量以m开头后接首字母大写驼峰式，公有成员变量和函数名规则一致

## 指针
- 严禁使用裸指针：用std::share_ptr、std::unique_ptr、std::weak_ptr代替

## 错误处理
使用错误码而不是异常的方式来返回错误
- 避免导出/public函数抛出异常，依赖的库可能会抛出异常，需要在导出函数中处理掉
- 不要在构造函数里抛出异常，构造函数均轻量级构造，通过提供Init/init函数来执行初始化并返回错误码
- 析构函数严禁抛出异常

## 代码风格
- 使用clang-format格式化代码保持风格一致

## 多态实现
多种多态实现方式，按性能高低排序
- 直接函数调用、模版方式
- 静态函数指针
- 虚函数动态调用
- std::function

## 锁
锁的粒度要足够小，只锁定需要的资源，资源获取到后立刻释放，处理好后再加锁处理结果

## 库使用

### log
底层使用的是plog，但是不要直接用plog的宏或函数(避免和libdatachannel依赖plog冲突)，使用sdk中封装的宏
- logVerb
- logDebug
- logInfo
- logWarn
- logError

### http请求
使用MainManager提供的封装，同步的用ApiPost，异步的用AsyncApiPost，推荐使用异步接口，避免直接使用HttpClient和ApiClient的接口

## 第三方依赖
- 慎重引入新的第三方依赖

# 编译
## make
第一次需要先编译依赖库
```bash
cd deps
make
```
后续改动时在相应目录下执行make，在dcdn目录下默认编译是生成libdcdn.a库。

新增加cpp文件需要在Makefile里添加对应的.o文件

## cmake

```bash
# 假设当前在项目根目录
mkdir build
cd build 
cmake ..
make -j8
```

# git hook 配置

## 安装pre-commit hook

```bash
cp hooks/pre-commit .git/hooks/
chmod +x .git/hooks/pre-commit
```

OR

```bash
git config --local core.hooksPath hooks
```
