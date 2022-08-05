set p=Simego.DataSync.Providers.DbSchema
"C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\MSBuild\Current\Bin\msbuild.exe" /t:Build /p:Configuration=Release /p:NoWarn=1591
rmdir ..\dist\ /S /Q
mkdir ..\dist\files\%p%
xcopy ..\src\%p%\bin\Release\net472\*.* ..\dist\files\%p%\*.* /y
cd ..\dist\files\
del .\%p%\Simego.DataSync.dll
del .\%p%\Simego.DataSync.Providers.Ado.dll
tar.exe -acf ..\%p%.zip *.*
cd ..\..\src


