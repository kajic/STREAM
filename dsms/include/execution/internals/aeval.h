#ifndef _AEVAL_
#define _AEVAL_

#include <string.h>

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

namespace Execution {
	
	enum AOp {		
		INT_ADD,
		INT_SUB,
		INT_MUL,
		INT_DIV,
		FLT_ADD,
		FLT_SUB,
		FLT_MUL,
		FLT_DIV,
		INT_CPY,
		FLT_CPY,
		CHR_CPY,
		BYT_CPY,
		INT_UMX,
		INT_UMN,
		FLT_UMX,
		FLT_UMN,
		INT_AVG,
		FLT_AVG
	};
	
	struct AInstr {
		AOp op;
		unsigned int r1;
		unsigned int c1;
		unsigned int r2;
		unsigned int c2;
		unsigned int dr;
		unsigned int dc;		
	};
	
	struct AEval {
	private:
		static const unsigned int MAX_INSTRS = 20;
		
		AInstr instrs [MAX_INSTRS];
		unsigned int numInstrs;
		char **roles;		
		
	public:
		AEval ();
		~AEval ();
		
		int addInstr (AInstr instr);
		int setEvalContext (EvalContext *evalContext);		

		inline void eval () {
			
			for (unsigned int i = 0 ; i < numInstrs ; i++) {
				
				switch (instrs [i].op) {
				case INT_ADD: 
					ILOC(instrs[i].dr, instrs[i].dc) =
						
						ILOC (instrs[i].r1, instrs[i].c1) +
						ILOC (instrs[i].r2, instrs[i].c2);
					break;
					
				case INT_SUB:
					ILOC(instrs[i].dr, instrs[i].dc) =
						
						ILOC (instrs[i].r1, instrs[i].c1) -
						ILOC (instrs[i].r2, instrs[i].c2);
					break;
					
				case INT_MUL:
					
					ILOC(instrs[i].dr, instrs[i].dc) =
						
						ILOC (instrs[i].r1, instrs[i].c1) *
						ILOC (instrs[i].r2, instrs[i].c2);
					break;
					
				case INT_DIV:
					
					ILOC(instrs[i].dr, instrs[i].dc) =
						
						ILOC (instrs[i].r1, instrs[i].c1) /
						ILOC (instrs[i].r2, instrs[i].c2);
					break;
					
				case FLT_ADD: 
					FLOC(instrs[i].dr, instrs[i].dc) =
						
						FLOC (instrs[i].r1, instrs[i].c1) +
						FLOC (instrs[i].r2, instrs[i].c2);
					break;
				
				case FLT_SUB:
					FLOC(instrs[i].dr, instrs[i].dc) =
					
						FLOC (instrs[i].r1, instrs[i].c1) -
						FLOC (instrs[i].r2, instrs[i].c2);
					break;
				
				case FLT_MUL:

					FLOC(instrs[i].dr, instrs[i].dc) =
					
						FLOC (instrs[i].r1, instrs[i].c1) *
						FLOC (instrs[i].r2, instrs[i].c2);
					break;

				case FLT_DIV:

					FLOC(instrs[i].dr, instrs[i].dc) =
					
						FLOC (instrs[i].r1, instrs[i].c1) /
						FLOC (instrs[i].r2, instrs[i].c2);
					break;
					
				case INT_CPY:
					ILOC (instrs[i].dr, instrs[i].dc) =
						ILOC (instrs[i].r1, instrs[i].c1);
					break;
					
				case FLT_CPY:
					FLOC (instrs[i].dr, instrs[i].dc) =
						FLOC (instrs[i].r1, instrs[i].c1);
					break;
					
				case CHR_CPY:
					strcpy (CLOC(instrs[i].dr, instrs[i].dc),
							CLOC(instrs[i].r1, instrs[i].c1));
					break;
					
				case BYT_CPY:
					BLOC (instrs[i].dr, instrs[i].dc) =
						BLOC (instrs[i].r1, instrs[i].c1);
					break;
					
				case INT_UMX:
					if (ILOC (instrs [i].r1, instrs [i].c1) <
						ILOC (instrs [i].r2, instrs [i].c2)) {
						
						ILOC (instrs [i].dr, instrs [i].dc) = 
							ILOC (instrs [i].r2, instrs [i].c2);
					}

					else {
						ILOC (instrs [i].dr, instrs [i].dc) =
							ILOC (instrs [i].r1, instrs [i].c1);
					}
					break;
					
				case INT_UMN:
					if (ILOC (instrs [i].r1, instrs [i].c1) >
						ILOC (instrs [i].r2, instrs [i].c2)) {
						
						ILOC (instrs [i].dr, instrs [i].dc) = 
							ILOC (instrs [i].r2, instrs [i].c2);
					}

					else {
						ILOC (instrs [i].dr, instrs [i].dc) =
							ILOC (instrs [i].r1, instrs [i].c1);
					}
					break;

				case FLT_UMX:
					if (FLOC (instrs [i].r1, instrs [i].c1) <
						FLOC (instrs [i].r2, instrs [i].c2)) {
						
						FLOC (instrs [i].dr, instrs [i].dc) = 
							FLOC (instrs [i].r2, instrs [i].c2);
					}

					else {
						FLOC (instrs [i].dr, instrs [i].dc) =
							FLOC (instrs [i].r1, instrs [i].c1);
					}
					break;
					
				case FLT_UMN:
					if (FLOC (instrs [i].r1, instrs [i].c1) <
						FLOC (instrs [i].r2, instrs [i].c2)) {
						
						FLOC (instrs [i].dr, instrs [i].dc) = 
							FLOC (instrs [i].r2, instrs [i].c2);
					}
					
					else {
						FLOC (instrs [i].dr, instrs [i].dc) =
							FLOC (instrs [i].r1, instrs [i].c1);
					}
					break;
					
				case INT_AVG:
					FLOC (instrs [i].dr, instrs[i].dc) =
						(1.0 * ILOC (instrs [i].r1, instrs [i].c1)) /
						(1.0 * ILOC (instrs [i].r2, instrs [i].c2));
					break;
					
				case FLT_AVG:
					FLOC (instrs [i].dr, instrs[i].dc) =
						FLOC (instrs [i].r1, instrs [i].c1) /
						(1.0 * ILOC (instrs [i].r2, instrs [i].c2));
					break;					
				}
			}
		}
	};
}


#endif
