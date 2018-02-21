FROM microsoft/dotnet:2.0-sdk-jessie

COPY . /build

WORKDIR /build
RUN dotnet restore

WORKDIR /build/NetCoreConsoleClient
RUN dotnet publish --framework netcoreapp2.0 --configuration Release --output /build/out NetCoreConsoleClient.csproj

ENTRYPOINT ["dotnet", "/build/out/NetCoreConsoleClient.dll"]
