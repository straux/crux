# clone shannah/jdeploy
# edit src/ca/weblite/jdeploy/JDeploy.java
#    private static final String[] CMD_ARRAY = { npm, "--otp=XXXXXX", "publish" };
#
#    private void publish() throws IOException {
#        try {
#            ProcessBuilder pb = new ProcessBuilder();
#            pb.inheritIO();
#/*            pb.command(npm, "publish");*/
#            pb.command(CMD_ARRAY);


# on first time, create ./target dir
# ./build.sh first
# on first time, copy target-package.json into ./target/package.json
# update package.json version
# open ~/jdeploy in netbeans
# then quickly...
# change --otp= in vim src/ca/weblite/jdeploy/JDeploy.java
# Run > Build in netbeans
# run this ./publish

cd target
node ~/jdeploy/bin/jdeploy.js publish
