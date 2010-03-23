#ifndef _BEVAL_
#define _BEVAL_

#include <string.h>

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#define CHECK(b) { if(!(b)) return false;}
#define EVAL(e) { if ((e)) (e)-> eval();}

namespace Execution {

	// The set of boolean operators
	enum BOp {
		INT_LT,
		INT_LE,
		INT_GT,
		INT_GE,
		INT_EQ,
		INT_NE,
		FLT_LT,
		FLT_LE,
		FLT_GT,
		FLT_GE,
		FLT_EQ,
		FLT_NE,
		CHR_LT,
		CHR_LE,
		CHR_GT,
		CHR_GE,
		CHR_EQ,
		CHR_NE,
		BYT_LT,
		BYT_LE,
		BYT_GT,
		BYT_GE,
		BYT_EQ,
		BYT_NE,
		C_INT_LT,
		C_INT_LE,
		C_INT_GT,
		C_INT_GE,
		C_INT_EQ,
		C_INT_NE,
		C_FLT_LT,
		C_FLT_LE,
		C_FLT_GT,
		C_FLT_GE,
		C_FLT_EQ,
		C_FLT_NE,
		C_CHR_LT,
		C_CHR_LE,
		C_CHR_GT,
		C_CHR_GE,
		C_CHR_EQ,
		C_CHR_NE,
		C_BYT_LT,
		C_BYT_LE,
		C_BYT_GT,
		C_BYT_GE,
		C_BYT_EQ,
		C_BYT_NE
	};
	
	struct BInstr {
		BOp            op;
		unsigned int   r1;
		Column         c1;
		unsigned int   r2;
		Column         c2;
		AEval         *e1;
		AEval         *e2;
	};
	
	class BEval {
	private:
		static const unsigned int MAX_INSTRS = 20;
		
		BInstr         instrs [MAX_INSTRS];
		unsigned       numInstrs;
		char         **roles;
		
	public:
		BEval ();
		~BEval ();
		
		int setEvalContext (EvalContext *evalContext);
		int addInstr (BInstr instr);
		
		inline bool eval () const {
			for (unsigned int i = 0 ; i < numInstrs ; i++) {
				switch (instrs [i].op) {
				case INT_LT: 
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) <
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case INT_LE:					
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) <=
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case INT_GT:
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) >
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case INT_GE:
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) >=
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;

				case INT_EQ:
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) ==
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case INT_NE:
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) !=
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
					
				case FLT_LT: 
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) <
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case FLT_LE:
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) <=
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case FLT_GT:
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) >
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case FLT_GE:
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) >=
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case FLT_EQ:
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) ==
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case FLT_NE:
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) !=
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;


					
					
				case BYT_LT: 
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) <
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case BYT_LE:
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) <=
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case BYT_GT:
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) >
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case BYT_GE:
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) >=
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case BYT_EQ:
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) ==
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case BYT_NE:
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) !=
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;


					

				case CHR_LT: 
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) < 0);
					break;

				case CHR_LE:
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) <= 0);
					break;

				case CHR_GT:
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) > 0);
					break;

				case CHR_GE:
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) >= 0);
					break;

				case CHR_EQ:
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) == 0);
					break;
					
				case CHR_NE:
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) != 0);
					break;


					
				case C_INT_LT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) <
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
										
				case C_INT_LE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) <=
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case C_INT_GT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) >
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case C_INT_GE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) >=
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_INT_EQ:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);

					CHECK(ILOC(instrs[i].r1, instrs[i].c1) ==
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case C_INT_NE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(ILOC(instrs[i].r1, instrs[i].c1) !=
						  ILOC(instrs[i].r2, instrs[i].c2));
					break;
					
					
				case C_FLT_LT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) <
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_FLT_LE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) <=
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_FLT_GT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) >
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_FLT_GE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) >=
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_FLT_EQ:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) ==
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_FLT_NE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(FLOC(instrs[i].r1, instrs[i].c1) !=
						  FLOC(instrs[i].r2, instrs[i].c2));
					break;


					
					
				case C_BYT_LT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) <
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_BYT_LE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) <=
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_BYT_GT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) >
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_BYT_GE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) >=
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;

				case C_BYT_EQ:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) ==
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;
					
				case C_BYT_NE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(BLOC(instrs[i].r1, instrs[i].c1) !=
						  BLOC(instrs[i].r2, instrs[i].c2));
					break;


					

				case C_CHR_LT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) < 0);
					break;

				case C_CHR_LE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) <= 0);
					break;

				case C_CHR_GT:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) > 0);
					break;

				case C_CHR_GE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) >= 0);
					break;

				case C_CHR_EQ:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) == 0);
					break;
					
				case C_CHR_NE:
					EVAL (instrs[i].e1);
					EVAL (instrs[i].e2);
					
					CHECK(strcmp (CLOC(instrs[i].r1, instrs[i].c1),
								  CLOC(instrs[i].r2, instrs[i].c2)) != 0);
					break;
					
				default:
					return false;
				}
			}
			
			return true;
		}
	};
}
#endif
