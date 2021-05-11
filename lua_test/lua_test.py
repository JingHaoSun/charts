import lupa

if __name__ == '__main__':
    luaEnv = lupa.LuaRuntime
    lua_file = open('./RunPython.lua').read()
    lua_test = luaEnv.execute(lua_file)
    print(lua_test.test())


