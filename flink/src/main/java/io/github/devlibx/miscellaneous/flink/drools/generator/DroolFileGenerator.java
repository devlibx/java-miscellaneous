package io.github.devlibx.miscellaneous.flink.drools.generator;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.comments.Comment;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.metamodel.FieldDeclarationMetaModel;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;
import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class DroolFileGenerator {

    private final String inputJavaFile;

    public DroolFileGenerator(String inputJavaFile) {
        this.inputJavaFile = inputJavaFile;

        CombinedTypeSolver combinedTypeSolver = new CombinedTypeSolver();
        combinedTypeSolver.add(new ReflectionTypeSolver());
        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(combinedTypeSolver);
        StaticJavaParser.getConfiguration().setSymbolResolver(symbolSolver);
    }

    public void generateOut(String outFile) throws IOException {
        String out = generateOut();
        File file = new File(outFile);
        FileUtils.writeStringToFile(file, out, Charset.defaultCharset());
    }

    public String generateOut() throws FileNotFoundException {
        CompilationUnit cu = StaticJavaParser.parse(new File(inputJavaFile));

        StringBuilder commonPropString = new StringBuilder();
        StringBuilder commonCodeForEachMethod = new StringBuilder();

        cu.accept(new VoidVisitorAdapter<Object>() {
            @Override
            public void visit(ClassOrInterfaceDeclaration n, Object arg) {
                super.visit(n, arg);
                n.getMembers().forEach(bodyDeclaration -> {
                    if (bodyDeclaration.getMetaModel() instanceof FieldDeclarationMetaModel) {
                        if (!bodyDeclaration.asFieldDeclaration().isTransient()) {
                            String str = bodyDeclaration.toString();
                            str = str.replace("public", "");
                            str = str.replace("static", "");
                            commonPropString.append(str).append("\n");
                        } else {
                            bodyDeclaration.asFieldDeclaration().getVariables().forEach(variableDeclarator -> {
                                if ("__commonCodeMarker__".equals(variableDeclarator.getName().asString())) {
                                    if (bodyDeclaration.getComment().isPresent()) {
                                        commonCodeForEachMethod.append(extractComments(bodyDeclaration.getComment().get()));
                                    }
                                }
                            });
                        }
                    }
                });
            }
        }, null);

        List<String> importStatements = new ArrayList<>();
        cu.getImports().forEach(importDeclaration -> {
            importStatements.add(importDeclaration.toString());
        });


        List<String> bodyStatements = new ArrayList<>();
        cu.accept(new VoidVisitorAdapter<Object>() {
            @Override
            public void visit(BlockStmt n, Object arg) {
                if (!n.getParentNode().isPresent()) return;

                Node parentNode = n.getParentNode().get();
                String body = parentNode.toString();


                String methodComments = parentNode.getComment().map(comment -> extractComments(comment)).orElse("");
                String ruleHeader = methodComments;
                String ruleBody = n.toString();

                String finalResult = "" +
                        ruleHeader;

                if (!Strings.isNullOrEmpty(commonPropString.toString())) {
                    finalResult += commonPropString.toString();
                }

                if (!Strings.isNullOrEmpty(commonCodeForEachMethod.toString())) {
                    finalResult += commonCodeForEachMethod.toString();
                }

                finalResult += ruleBody + "\n" +
                        "end\n\n\n";
                bodyStatements.add(finalResult);
            }
        }, null);

        StringBuilder sb = new StringBuilder();
        importStatements.forEach(sb::append);
        bodyStatements.forEach(sb::append);
        return sb.toString();
    }

    private String extractComments(Comment comment) {
        try {
            String text = comment.asJavadocComment().parse().toText();
            text = text.replace("<pre>", "");
            text = text.replace("</pre>", "");
            return text;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}
