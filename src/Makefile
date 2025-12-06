# Definim compilatorul si classpath-ul
# CLASSPATH include directorul curent (.) si toate jar-urile din folderul libs/
CLASSPATH = .:libs/*

# Regula 'build': compileaza sursele
# -cp specifica unde sa caute bibliotecile (Jackson)
build:
	javac -cp "$(CLASSPATH)" *.java

# Regula 'run': ruleaza programul
# Checker-ul va apela: make run ARGS="<threads> <input> <aux>"
# Noi preluam variabila $(ARGS) si o dam programului Java
run:
	java -cp "$(CLASSPATH)" Tema1 $(ARGS)

# Regula 'clean': sterge fisierele compilate (.class)
clean:
	rm -f *.class
