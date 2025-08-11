# DCDN sdk develop

# 开发规范
- 跨平台：只使用C++17语言标准和依赖库封装好的接口
- 命名规范：合理使用命名空间，类名和public函数用首字母大写驼峰式，protected和private函数用首字母小写驼峰式，私有成员变量以m开头后接首字母大写驼峰式，公有成员变量和函数名规则一致
- 严禁使用裸指针：用std::share_ptr、std::unique_ptr、std::weak_ptr代替
- 避免导出/public函数抛出异常
- 使用clang-format格式化代码保持风格一致
- 慎重引入新的第三方依赖

# 编译示例
```bash
# 假设当前在项目根目录
mkdir build
cd build 
cmake ..
make -j8
```